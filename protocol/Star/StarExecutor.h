//
// Created by Yi Lu on 9/7/18.
//

#pragma once

#include "core/Partitioner.h"

#include "common/BufferedFileWriter.h"
#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Star/Star.h"
#include "protocol/Star/StarQueryNum.h"

#include <chrono>
#include <queue>

namespace star
{

template <class Workload> class StarExecutor : public Worker {
    public:
	using WorkloadType = Workload;
	using DatabaseType = typename WorkloadType::DatabaseType;
	using StorageType = typename WorkloadType::StorageType;
	using TransactionType = typename WorkloadType::TransactionType;
	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;

	using ProtocolType = Star<DatabaseType>;

	using MessageType = StarMessage;
	using MessageFactoryType = StarMessageFactory;
	using MessageHandlerType = StarMessageHandler<DatabaseType>;

	StarExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db, const ContextType &context, uint32_t &batch_size,
		     std::atomic<uint32_t> &worker_status, std::atomic<uint32_t> &n_complete_workers, std::atomic<uint32_t> &n_started_workers)
		: Worker(coordinator_id, id)
		, db(db)
		, context(context)
		, batch_size(batch_size)
		, s_partitioner(std::make_unique<StarSPartitioner>(coordinator_id, context.coordinator_num))
		, c_partitioner(std::make_unique<StarCPartitioner>(coordinator_id, context.coordinator_num))
		, random(reinterpret_cast<uint64_t>(this))
		, worker_status(worker_status)
		, n_complete_workers(n_complete_workers)
		, n_started_workers(n_started_workers)
		, delay(std::make_unique<SameDelay>(coordinator_id, context.coordinator_num, context.delay_time))
	{
		for (auto i = 0u; i < context.coordinator_num; i++) {
			sync_messages.emplace_back(std::make_unique<Message>());
			init_message(sync_messages[i].get(), i);

			async_messages.emplace_back(std::make_unique<Message>());
			init_message(async_messages[i].get(), i);
		}

		messageHandlers = MessageHandlerType::get_message_handlers();

		if (context.log_path != "") {
			std::string filename = context.log_path + "_" + std::to_string(id) + ".txt";
			logger = std::make_unique<BufferedFileWriter>(filename.c_str(), context.emulated_persist_latency);
		}
	}

	void start() override
	{
		LOG(INFO) << "Executor " << id << " starts.";

		// C-Phase to S-Phase, to C-phase ...

		for (;;) {
			ExecutorStatus status;

			do {
				status = static_cast<ExecutorStatus>(worker_status.load());

				if (status == ExecutorStatus::EXIT) {
					// commit transaction in s_phase;
					commit_transactions();
					LOG(INFO) << "Executor " << id << " exits.";
					return;
				}
			} while (status != ExecutorStatus::C_PHASE);

			// commit transaction in s_phase;
			commit_transactions();

			// c_phase

			if (coordinator_id == 0) {
				n_started_workers.fetch_add(1);
				run_transaction(ExecutorStatus::C_PHASE);
				n_complete_workers.fetch_add(1);

			} else {
				n_started_workers.fetch_add(1);

				while (static_cast<ExecutorStatus>(worker_status.load()) == ExecutorStatus::C_PHASE) {
					process_request();
				}

				// process replication request after all workers stop.
				process_request();
				n_complete_workers.fetch_add(1);
			}

			// wait to s_phase

			while (static_cast<ExecutorStatus>(worker_status.load()) != ExecutorStatus::S_PHASE) {
				std::this_thread::yield();
			}

			// commit transaction in c_phase;
			commit_transactions();

			// s_phase

			n_started_workers.fetch_add(1);

			run_transaction(ExecutorStatus::S_PHASE);

			n_complete_workers.fetch_add(1);

			// once all workers are stop, we need to process the replication
			// requests

			while (static_cast<ExecutorStatus>(worker_status.load()) == ExecutorStatus::S_PHASE) {
				process_request();
			}

			// n_complete_workers has been cleared
			process_request();
			n_complete_workers.fetch_add(1);
		}
	}

	void commit_transactions()
	{
		while (!q.empty()) {
			auto &ptr = q.front();
			auto latency = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - ptr->startTime).count();
			percentile.add(latency);
			q.pop();
		}
	}

	std::size_t get_partition_id(ExecutorStatus status)
	{
		std::size_t partition_id;

		if (status == ExecutorStatus::C_PHASE) {
			CHECK(coordinator_id == 0);
			CHECK(context.partition_num % context.worker_num == 0);
			auto partition_num_per_thread = context.partition_num / context.worker_num;
			partition_id = id * partition_num_per_thread + random.uniform_dist(0, partition_num_per_thread - 1);
		} else if (status == ExecutorStatus::S_PHASE) {
			partition_id = id * context.coordinator_num + coordinator_id;
		} else {
			CHECK(false);
		}

		return partition_id;
	}

	void run_transaction(ExecutorStatus status)
	{
		std::size_t query_num = 0;

		Partitioner *partitioner = nullptr;

		ContextType phase_context;

		if (status == ExecutorStatus::C_PHASE) {
			partitioner = c_partitioner.get();
			query_num = StarQueryNum<ContextType>::get_c_phase_query_num(context, batch_size);
			phase_context = context.get_cross_partition_context();
		} else if (status == ExecutorStatus::S_PHASE) {
			partitioner = s_partitioner.get();
			query_num = StarQueryNum<ContextType>::get_s_phase_query_num(context, batch_size);
			phase_context = context.get_single_partition_context();
		} else {
			CHECK(false);
		}

		ProtocolType protocol(db, phase_context, *partitioner);
		WorkloadType workload(coordinator_id, db, random, *partitioner);

		uint64_t last_seed = 0;
		// LOG(INFO) << "query_num " << query_num << " batch_size " << batch_size;
		for (auto i = 0u; i < query_num; i++) {
			bool retry_transaction = false;

			do {
				process_request();
				last_seed = random.get_seed();

				if (retry_transaction) {
					transaction->reset();
				} else {
					std::size_t partition_id = get_partition_id(status);
					transaction = workload.next_transaction(phase_context, partition_id, this->id);
					setupHandlers(*transaction, protocol);
				}

				auto result = transaction->execute(id);
				if (result == TransactionResult::READY_TO_COMMIT) {
					bool commit = protocol.commit(*transaction, sync_messages, async_messages);
					n_network_size.fetch_add(transaction->network_size);
					if (commit) {
						n_commit.fetch_add(1);
						retry_transaction = false;
						q.push(std::move(transaction));
					} else {
						if (transaction->abort_lock) {
							n_abort_lock.fetch_add(1);
						} else {
							DCHECK(transaction->abort_read_validation);
							n_abort_read_validation.fetch_add(1);
						}
						random.set_seed(last_seed);
						retry_transaction = true;
					}
				} else {
					n_abort_no_retry.fetch_add(1);
				}
			} while (retry_transaction);

			if (i % phase_context.batch_flush == 0) {
				flush_async_messages();
			}
		}
		// LOG(INFO) << "query_num " << query_num << " Done";
		flush_async_messages();
	}

	void onExit() override
	{
		LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50) << " us (50%) " << percentile.nth(75) << " us (75%) " << percentile.nth(95)
			  << " us (95%) " << percentile.nth(99) << " us (99%).";

		if (logger != nullptr) {
			logger->close();
		}
	}

	void push_message(Message *message) override
	{
		in_queue.push(message);
	}

	void push_replica_message(Message *message) override
	{
		DCHECK(false);
		in_queue.push(message);
	}

	Message *pop_message() override
	{
		if (out_queue.empty())
			return nullptr;

		Message *message = out_queue.front();

		if (delay->delay_enabled()) {
			auto now = std::chrono::steady_clock::now();
			if (std::chrono::duration_cast<std::chrono::microseconds>(now - message->time).count() < delay->message_delay()) {
				return nullptr;
			}
		}

		bool ok = out_queue.pop();
		CHECK(ok);

		return message;
	}

    private:
	std::size_t process_request()
	{
		std::size_t size = 0;

		while (!in_queue.empty()) {
			std::unique_ptr<Message> message(in_queue.front());
			bool ok = in_queue.pop();
			CHECK(ok);

			for (auto it = message->begin(); it != message->end(); it++) {
				MessagePiece messagePiece = *it;
				auto type = messagePiece.get_message_type();
				DCHECK(type < messageHandlers.size());

				messageHandlers[type](messagePiece, *sync_messages[message->get_source_node_id()], db, transaction.get());
				if (logger) {
					logger->write(messagePiece.toStringPiece().data(), messagePiece.get_message_length());
				}
			}

			size += message->get_message_count();
			flush_sync_messages();
		}
		return size;
	}

	void setupHandlers(TransactionType &txn, ProtocolType &protocol)
	{
		txn.readRequestHandler = [&protocol](std::size_t table_id, std::size_t partition_id, uint32_t key_offset, const void *key, void *value,
						     bool local_index_read) -> uint64_t { return protocol.search(table_id, partition_id, key, value); };

		txn.remote_request_handler = [this](std::size_t) { return this->process_request(); };
		txn.message_flusher = [this]() { this->flush_sync_messages(); };
	}

	void flush_messages(std::vector<std::unique_ptr<Message> > &messages)
	{
		for (auto i = 0u; i < messages.size(); i++) {
			if (i == coordinator_id) {
				continue;
			}

			if (messages[i]->get_message_count() == 0) {
				continue;
			}

			auto message = messages[i].release();

			out_queue.push(message);
			messages[i] = std::make_unique<Message>();
			init_message(messages[i].get(), i);
		}
	}

	void flush_sync_messages()
	{
		flush_messages(sync_messages);
	}

	void flush_async_messages()
	{
		flush_messages(async_messages);
	}

	void init_message(Message *message, std::size_t dest_node_id)
	{
		message->set_source_node_id(coordinator_id);
		message->set_dest_node_id(dest_node_id);
		message->set_worker_id(id);
	}

    private:
	DatabaseType &db;
	ContextType context;
	uint32_t &batch_size;
	std::unique_ptr<Partitioner> s_partitioner, c_partitioner;
	RandomType random;
	std::atomic<uint32_t> &worker_status;
	std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
	std::unique_ptr<Delay> delay;
	std::unique_ptr<BufferedFileWriter> logger;
	Percentile<int64_t> percentile;
	std::unique_ptr<TransactionType> transaction;
	// transaction only commit in a single group
	std::queue<std::unique_ptr<TransactionType> > q;
	std::vector<std::unique_ptr<Message> > sync_messages, async_messages;
	std::vector<std::function<void(MessagePiece, Message &, DatabaseType &, TransactionType *)> > messageHandlers;
	LockfreeQueue<Message *> in_queue, out_queue;
};
} // namespace star