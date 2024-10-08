//
// Created by Yi Lu on 8/29/18.
//

#pragma once

#include "common/Percentile.h"
#include "common/WALLogger.h"
#include "common/BufferedFileWriter.h"
#include "core/ControlMessage.h"
#include "core/Defs.h"
#include "core/Delay.h"
#include "core/Partitioner.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include <chrono>
#include <thread>

namespace star
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
			messages.emplace_back(std::make_unique<Message>());
			init_message(messages[i].get(), i);
		}

		messageHandlers = MessageHandlerType::get_message_handlers();
		message_stats.resize(messageHandlers.size(), 0);
		message_sizes.resize(messageHandlers.size(), 0);

		if (context.logger) {
			DCHECK(context.log_path != "");
			logger = context.logger;
		} else {
			if (context.log_path != "") {
				std::string redo_filename = context.log_path + "_" + std::to_string(id) + ".txt";
				logger = new SimpleWALLogger(redo_filename.c_str(), context.emulated_persist_latency);
			}
		}
	}

	~Executor() = default;

	void start() override
	{
		LOG(INFO) << "Executor " << id << " starts.";

                cxl_memory.init_cxlalloc_for_given_thread(context.worker_num + 1, id + 1, context.coordinator_num, context.coordinator_id);

		uint64_t last_seed = 0;

		ExecutorStatus status;

		while ((status = static_cast<ExecutorStatus>(worker_status.load())) != ExecutorStatus::START) {
			std::this_thread::yield();
		}

		n_started_workers.fetch_add(1);
		bool retry_transaction = false;

		// auto startTime = std::chrono::steady_clock::now();
		auto t = workload.next_transaction(context, 0, this->id);
		auto dummy_transaction = t.release();
		setupHandlers(*dummy_transaction);
		do {
			auto tmp_transaction = transaction.get();
			bool replace_with_dummy = tmp_transaction == nullptr;
			if (replace_with_dummy) {
				// Hack: Make sure transaction is not nullptr
				transaction.reset(dummy_transaction);
			}
			process_request();
			if (replace_with_dummy) {
				// swap it back.
				transaction.reset(tmp_transaction);
			}

			if (!partitioner->is_backup()) {
				// backup node stands by for replication
				last_seed = random.get_seed();

				if (retry_transaction) {
					transaction->reset();
				} else {
					auto partition_id = get_partition_id();

					transaction = workload.next_transaction(context, partition_id, this->id);
					// startTime = std::chrono::steady_clock::now();
					setupHandlers(*transaction);
				}

				auto result = transaction->execute(id);
				if (result == TransactionResult::READY_TO_COMMIT) {
					bool commit;
					{
						ScopedTimer t([&, this](uint64_t us) {
							if (commit) {
								this->transaction->record_commit_work_time(us);
							} else {
								auto ltc = std::chrono::duration_cast<std::chrono::nanoseconds>(
										   std::chrono::steady_clock::now() - transaction->startTime)
										   .count();
								this->transaction->set_stall_time(ltc);
							}
						});
						commit = protocol.commit(*transaction, messages);
					}
					auto ltc =
						std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - transaction->startTime)
							.count();
					commit_latency.add(ltc);
					n_network_size.fetch_add(transaction->network_size);
					if (commit) {
                                                // for correctness test
                                                db.global_total_commit.fetch_add(1);

						n_commit.fetch_add(1);
						if (transaction->si_in_serializable) {
							n_si_in_serializable.fetch_add(1);
						}
						if (transaction->local_validated) {
							n_local.fetch_add(1);
						}
						retry_transaction = false;
						auto latency = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() -
														     transaction->startTime)
								       .count();
						percentile.add(latency);
						if (transaction->is_single_partition() == false) {
							dist_latency.add(latency);
						} else {
							local_latency.add(latency);
						}
						record_txn_breakdown_stats(*transaction.get());
					} else {
						if (transaction->abort_lock) {
							n_abort_lock.fetch_add(1);
						} else {
							DCHECK(transaction->abort_read_validation);
							n_abort_read_validation.fetch_add(1);
						}
						if (context.sleep_on_retry) {
							std::this_thread::sleep_for(std::chrono::microseconds(random.uniform_dist(0, context.sleep_time)));
						}
						random.set_seed(last_seed);
						retry_transaction = true;
					}
				} else if (result == TransactionResult::ABORT_NORETRY) {
					protocol.abort(*transaction, messages);
					n_abort_no_retry.fetch_add(1);
				} else {
                                        CHECK(result == TransactionResult::ABORT);
                                        protocol.abort(*transaction, messages);
                                        if (transaction->abort_lock) {
                                                n_abort_lock.fetch_add(1);
                                        } else {
                                                DCHECK(transaction->abort_read_validation);
                                                n_abort_read_validation.fetch_add(1);
                                        }
                                        if (context.sleep_on_retry) {
                                                std::this_thread::sleep_for(std::chrono::microseconds(random.uniform_dist(0, context.sleep_time)));
                                        }
                                        random.set_seed(last_seed);
                                        retry_transaction = true;
                                }
			}

			status = static_cast<ExecutorStatus>(worker_status.load());
		} while (status != ExecutorStatus::STOP);

		n_complete_workers.fetch_add(1);

		// once all workers are stop, we need to process the replication
		// requests

		while (static_cast<ExecutorStatus>(worker_status.load()) != ExecutorStatus::CLEANUP) {
			process_request();
		}

		process_request();
		n_complete_workers.fetch_add(1);

		LOG(INFO) << "Executor " << id << " exits.";
	}

	void onExit() override
	{
		LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50) << " us (50%) " << percentile.nth(75) << " us (75%) " << percentile.nth(95)
			  << " us (95%) " << percentile.nth(99) << " us (99%). dist txn latency: " << dist_latency.nth(50) << " us (50%) "
			  << dist_latency.nth(75) << " us (75%) " << dist_latency.nth(95) << " us (95%) " << dist_latency.nth(99) << " us (99%) "
			  << " avg " << dist_latency.avg() << " us "
			  << ". local txn latency: " << local_latency.nth(50) << " us (50%) " << local_latency.nth(75) << " us (75%) " << local_latency.nth(95)
			  << " us (95%) " << local_latency.nth(99) << " us (99%) "
			  << " avg " << local_latency.avg() << " us."
			  << " txn commit latency: " << commit_latency.nth(50) << " us (50%) " << commit_latency.nth(75) << " us (75%) "
			  << commit_latency.nth(95) << " us (95%) " << commit_latency.nth(99) << " us (99%) "
			  << "avg " << commit_latency.avg() << " us.\n"
			  << " LOCAL txn stall " << this->local_txn_stall_time_pct.avg() << " us, "
			  << " local_work " << this->local_txn_local_work_time_pct.avg() << " us, "
			  << " remote_work " << this->local_txn_remote_work_time_pct.avg() << " us, "
			  << " commit_work " << this->local_txn_commit_work_time_pct.avg() << " us, "
			  << " commit_prepare " << this->local_txn_commit_prepare_time_pct.avg() << " us, "
			  << " commit_persistence " << this->local_txn_commit_persistence_time_pct.avg() << " us, "
			  << " commit_write_back " << this->local_txn_commit_write_back_time_pct.avg() << " us, "
			  << " commit_replication " << this->local_txn_commit_replication_time_pct.avg() << " us, "
			  << " commit_release_lock " << this->local_txn_commit_unlock_time_pct.avg() << " us \n"
			  << " DIST txn stall " << this->dist_txn_stall_time_pct.avg() << " us, "
			  << " local_work " << this->dist_txn_local_work_time_pct.avg() << " us, "
			  << " remote_work " << this->dist_txn_remote_work_time_pct.avg() << " us, "
			  << " commit_work " << this->dist_txn_commit_work_time_pct.avg() << " us, "
			  << " commit_prepare " << this->dist_txn_commit_prepare_time_pct.avg() << " us, "
			  << " commit_persistence " << this->dist_txn_commit_persistence_time_pct.avg() << " us, "
			  << " commit_write_back " << this->dist_txn_commit_write_back_time_pct.avg() << " us, "
			  << " commit_replication " << this->dist_txn_commit_replication_time_pct.avg() << " us, "
			  << " commit_release_lock " << this->dist_txn_commit_unlock_time_pct.avg() << " us \n";

		if (id == 0) {
			for (auto i = 0u; i < message_stats.size(); i++) {
				LOG(INFO) << "message stats, type: " << i << " count: " << message_stats[i] << " total size: " << message_sizes[i];
			}
			percentile.save_cdf(context.cdf_path);

                        // do a correctness test for the current workload
                        workload.perform_correctness_test(context);
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
		CHECK(false);
	}

	Message *pop_message() override
	{
		if (out_queue.empty())
			return nullptr;

		Message *message = out_queue.front();

		// if (delay->delay_enabled()) {
		//   auto now = std::chrono::steady_clock::now();
		//   if (std::chrono::duration_cast<std::chrono::microseconds>(now -
		//                                                             message->time)
		//           .count() < delay->message_delay()) {
		//     return nullptr;
		//   }
		// }

		bool ok = out_queue.pop();
		CHECK(ok);

		return message;
	}

	std::size_t process_request()
	{
		std::size_t size = 0;

		while (!in_queue.empty()) {
			size++;
			std::unique_ptr<Message> message(in_queue.front());
			bool ok = in_queue.pop();
			CHECK(ok);

			for (auto it = message->begin(); it != message->end(); it++) {
				MessagePiece messagePiece = *it;
				auto type = messagePiece.get_message_type();
				DCHECK(type < messageHandlers.size());
				ITable *table = db.find_table(messagePiece.get_table_id(), messagePiece.get_partition_id());

				messageHandlers[type](messagePiece, *messages[message->get_source_node_id()], *table, transaction.get());

				message_stats[type]++;
				message_sizes[type] += messagePiece.get_message_length();
			}

			size += message->get_message_count();
			flush_messages();
		}
		return size;
	}

	virtual void setupHandlers(TransactionType &txn) = 0;

	virtual void flush_messages()
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
			message->set_put_to_out_queue_time(Time::now());

			messages[i] = std::make_unique<Message>();
			init_message(messages[i].get(), i);
		}
	}

	void init_message(Message *message, std::size_t dest_node_id)
	{
		message->set_source_node_id(coordinator_id);
		message->set_dest_node_id(dest_node_id);
		message->set_worker_id(id);
	}

    public:
	DatabaseType &db;
	ContextType context;
	std::atomic<uint32_t> &worker_status;
	std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
	std::unique_ptr<Partitioner> partitioner;
	RandomType random;
	ProtocolType protocol;
	WorkloadType workload;
	std::unique_ptr<Delay> delay;
	Percentile<int64_t> percentile, dist_latency, local_latency, commit_latency;
	Percentile<uint64_t> local_txn_stall_time_pct, local_txn_commit_work_time_pct, local_txn_commit_persistence_time_pct, local_txn_commit_prepare_time_pct,
		local_txn_commit_replication_time_pct, local_txn_commit_write_back_time_pct, local_txn_commit_unlock_time_pct, local_txn_local_work_time_pct,
		local_txn_remote_work_time_pct;
	Percentile<uint64_t> dist_txn_stall_time_pct, dist_txn_commit_work_time_pct, dist_txn_commit_persistence_time_pct, dist_txn_commit_prepare_time_pct,
		dist_txn_commit_write_back_time_pct, dist_txn_commit_unlock_time_pct, dist_txn_local_work_time_pct, dist_txn_commit_replication_time_pct,
		dist_txn_remote_work_time_pct;
	std::unique_ptr<TransactionType> transaction;
	std::unique_ptr<TransactionType> transaction_replica;
	std::vector<std::unique_ptr<Message> > messages;
	std::vector<std::function<void(MessagePiece, Message &, ITable &, TransactionType *)> > messageHandlers;
	std::vector<std::size_t> message_stats, message_sizes;
	LockfreeQueue<Message *> in_queue;
	char pad[64];
	LockfreeQueue<Message *> out_queue;

	WALLogger *logger = nullptr;
	void record_txn_breakdown_stats(TransactionType &txn)
	{
		if (txn.is_single_partition()) {
			local_txn_stall_time_pct.add(txn.get_stall_time());
			local_txn_commit_work_time_pct.add(txn.get_commit_work_time());
			local_txn_commit_write_back_time_pct.add(txn.get_commit_write_back_time());
			local_txn_commit_unlock_time_pct.add(txn.get_commit_unlock_time());
			local_txn_local_work_time_pct.add(txn.get_local_work_time());
			local_txn_remote_work_time_pct.add(txn.get_remote_work_time());
			local_txn_commit_persistence_time_pct.add(txn.get_commit_persistence_time());
			local_txn_commit_prepare_time_pct.add(txn.get_commit_prepare_time());
			local_txn_commit_replication_time_pct.add(txn.get_commit_replication_time());
		} else {
			dist_txn_stall_time_pct.add(txn.get_stall_time());
			dist_txn_commit_work_time_pct.add(txn.get_commit_work_time());
			dist_txn_commit_write_back_time_pct.add(txn.get_commit_write_back_time());
			dist_txn_commit_unlock_time_pct.add(txn.get_commit_unlock_time());
			dist_txn_local_work_time_pct.add(txn.get_local_work_time());
			dist_txn_remote_work_time_pct.add(txn.get_remote_work_time());
			dist_txn_commit_persistence_time_pct.add(txn.get_commit_persistence_time());
			dist_txn_commit_prepare_time_pct.add(txn.get_commit_prepare_time());
			dist_txn_commit_replication_time_pct.add(txn.get_commit_replication_time());
		}
	}
};
} // namespace star