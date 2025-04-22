//
// Created by Yi Lu on 9/10/18.
//

#pragma once

#include "common/Percentile.h"
#include "core/ControlMessage.h"
#include "core/Defs.h"
#include "core/Delay.h"
#include "core/Partitioner.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include <chrono>

namespace star
{
namespace group_commit
{

template <class Workload, class Protocol> class Executor : public Worker {
    public:
	using WorkloadType = Workload;
	using ProtocolType = Protocol;
	using DatabaseType = typename WorkloadType::DatabaseType;
	using TransactionType = typename WorkloadType::TransactionType;
	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;
	using MessageType = typename ProtocolType::MessageType;
	using MessageFactoryType = typename ProtocolType::MessageFactoryType;
	using MessageHandlerType = typename ProtocolType::MessageHandlerType;

	using StorageType = typename WorkloadType::StorageType;

	Executor(std::size_t coordinator_id, std::size_t id, DatabaseType &db, const ContextType &context, std::atomic<uint32_t> &worker_status,
		 std::atomic<uint32_t> &n_complete_workers, std::atomic<uint32_t> &n_started_workers)
		: Worker(coordinator_id, id)
		, db(db)
		, context(context)
		, worker_status(worker_status)
		, n_complete_workers(n_complete_workers)
		, n_started_workers(n_started_workers)
		, partitioner(PartitionerFactory::create_partitioner(context.partitioner, coordinator_id, context.coordinator_num))
		, random(reinterpret_cast<uint64_t>(this))
		, protocol(db, context, *partitioner)
		, workload(coordinator_id, db, random, *partitioner)
		, delay(std::make_unique<SameDelay>(coordinator_id, context.coordinator_num, context.delay_time))
	{
		for (auto i = 0u; i < context.coordinator_num; i++) {
			sync_messages.emplace_back(std::make_unique<Message>());
			init_message(sync_messages[i].get(), i);

			async_messages.emplace_back(std::make_unique<Message>());
			init_message(async_messages[i].get(), i);
		}

		messageHandlers = MessageHandlerType::get_message_handlers();
		message_stats.resize(messageHandlers.size(), 0);
		message_sizes.resize(messageHandlers.size(), 0);
	}

	void start() override
	{
		LOG(INFO) << "Executor " << id << " starts.";

		uint64_t last_seed = 0;

		// transaction only commit in a single group

		std::queue<std::unique_ptr<TransactionType> > q;
		std::size_t count = 0;

		for (;;) {
			ExecutorStatus status;
			do {
				status = static_cast<ExecutorStatus>(worker_status.load());

				if (status == ExecutorStatus::EXIT) {
					LOG(INFO) << "Executor " << id << " exits.";
					return;
				}
			} while (status != ExecutorStatus::START);

			while (!q.empty()) {
				auto &ptr = q.front();
				auto latency = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - ptr->startTime).count();
				commit_latency.add(latency);
				q.pop();
			}

			n_started_workers.fetch_add(1);

			bool retry_transaction = false;

			do {
				count++;

				process_request();

				if (!partitioner->is_backup()) {
					// backup node stands by for replication
					last_seed = random.get_seed();

					if (retry_transaction) {
						transaction->reset();
					} else {
						auto partition_id = get_partition_id();

						transaction = workload.next_transaction(context, partition_id, this->id);
						setupHandlers(*transaction);
					}

					auto result = transaction->execute(id);
					if (result == TransactionResult::READY_TO_COMMIT) {
						bool commit = protocol.commit(*transaction, sync_messages, async_messages);
						n_network_size.fetch_add(transaction->network_size);
						if (commit) {
							n_commit.fetch_add(1);
							if (transaction->si_in_serializable) {
								n_si_in_serializable.fetch_add(1);
							}
							if (transaction->local_validated) {
								n_local.fetch_add(1);
							}

							auto latency = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() -
															     transaction->startTime)
									       .count();
							write_latency.add(latency);
							if (transaction->distributed_transaction) {
								dist_latency.add(latency);
							} else {
								local_latency.add(latency);
							}
							retry_transaction = false;
							q.push(std::move(transaction));
						} else {
							if (transaction->abort_lock) {
								n_abort_lock.fetch_add(1);
							} else {
								DCHECK(transaction->abort_read_validation);
								n_abort_read_validation.fetch_add(1);
							}
							if (context.sleep_on_retry) {
								std::this_thread::sleep_for(
									std::chrono::microseconds(random.uniform_dist(0, context.sleep_time)));
							}
							random.set_seed(last_seed);
							retry_transaction = true;
						}
					} else {
						protocol.abort(*transaction, sync_messages, async_messages);
						n_abort_no_retry.fetch_add(1);
					}

					if (count % context.batch_flush == 0) {
						flush_async_messages();
					}
				}

				status = static_cast<ExecutorStatus>(worker_status.load());
			} while (status != ExecutorStatus::STOP);

			flush_async_messages();

			n_complete_workers.fetch_add(1);

			// once all workers are stop, we need to process the replication
			// requests

			while (static_cast<ExecutorStatus>(worker_status.load()) != ExecutorStatus::CLEANUP) {
				process_request();
			}

			process_request();
			n_complete_workers.fetch_add(1);
		}
	}

	void onExit() override
	{
		LOG(INFO) << "Worker " << id << " latency: " << commit_latency.nth(50) << " us (50%) " << commit_latency.nth(75) << " us (75%) "
			  << commit_latency.nth(95) << " us (95%) " << commit_latency.nth(99) << " us (99%). write latency: " << write_latency.nth(50)
			  << " us (50%) " << write_latency.nth(75) << " us (75%) " << write_latency.nth(95) << " us (95%) " << write_latency.nth(99)
			  << " us (99%). dist txn latency: " << dist_latency.nth(50) << " us (50%) " << dist_latency.nth(75) << " us (75%) "
			  << dist_latency.nth(95) << " us (95%) " << dist_latency.nth(99) << " us (99%). local txn latency: " << local_latency.nth(50)
			  << " us (50%) " << local_latency.nth(75) << " us (75%) " << local_latency.nth(95) << " us (95%) " << local_latency.nth(99)
			  << " us (99%).";

		if (id == 0) {
			for (auto i = 0u; i < message_stats.size(); i++) {
				LOG(INFO) << "message stats, type: " << i << " count: " << message_stats[i] << " total size: " << message_sizes[i];
			}
			write_latency.save_cdf(context.cdf_path);
		}
	}

	std::size_t get_partition_id()
	{
		std::size_t partition_id;

		if (context.partitioner == "pb") {
			partition_id = random.uniform_dist(0, context.partition_num - 1);
		} else {
			auto partition_num_per_node = context.partition_num / context.coordinator_num;
			partition_id = random.uniform_dist(0, partition_num_per_node - 1) * context.coordinator_num + coordinator_id;
		}
		CHECK(partitioner->has_master_partition(partition_id));
		return partition_id;
	}

	void push_message(Message *message) override
	{
		in_queue.push(message);
	}

	void push_replica_message(Message *message) override
	{
		DCHECK(false);
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
				ITable *table = db.find_table(messagePiece.get_table_id(), messagePiece.get_partition_id());

				messageHandlers[type](messagePiece, *sync_messages[message->get_source_node_id()], *table, transaction.get());
				message_stats[type]++;
				message_sizes[type] += messagePiece.get_message_length();
			}

			size += message->get_message_count();
			flush_sync_messages();
		}
		return size;
	}

	virtual void setupHandlers(TransactionType &txn) = 0;

    protected:
	void flush_messages(std::vector<std::unique_ptr<Message> > &messages)
	{
		for (auto i = 0u; i < messages.size(); i++) {
			if (i == coordinator_id) {
				continue;
			}

			if (messages[i]->get_message_count() == 0) {
				continue;
			}

			if (context.use_output_thread == true) {
			        out_queue.push(messages[i].release());
                                // message is reclaimed by the output thread
                        } else {
                                cxl_transport->send(messages[i].get());
                                // must reclaim the message here - otherwise memory leakage would occur
                                // we do it by not releasing it
                        }

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

    protected:
	DatabaseType &db;
	ContextType context;
	std::atomic<uint32_t> &worker_status;
	std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
	std::unique_ptr<Partitioner> partitioner;
	RandomType random;
	ProtocolType protocol;
	WorkloadType workload;
	std::unique_ptr<Delay> delay;
	Percentile<int64_t> commit_latency, write_latency;
	Percentile<int64_t> dist_latency, local_latency;
	std::unique_ptr<TransactionType> transaction;
	std::vector<std::unique_ptr<Message> > sync_messages, async_messages;
	std::vector<std::function<void(MessagePiece, Message &, ITable &, TransactionType *)> > messageHandlers;
	std::vector<std::size_t> message_stats, message_sizes;
	LockfreeQueue<Message *> in_queue, out_queue;
};
} // namespace group_commit

} // namespace star