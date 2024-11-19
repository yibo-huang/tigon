//
// Created by Yi Lu on 9/13/18.
//

#pragma once

#include "core/Partitioner.h"

#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Calvin/Calvin.h"
#include "protocol/Calvin/CalvinHelper.h"
#include "protocol/Calvin/CalvinMessage.h"

#include <atomic>
#include <chrono>
#include <thread>

namespace star
{

template <class Workload> class CalvinExecutor : public Worker {
    public:
	using WorkloadType = Workload;
	using DatabaseType = typename WorkloadType::DatabaseType;
	using StorageType = typename WorkloadType::StorageType;

	using TransactionType = CalvinTransaction;
	std::vector<TransactionType::TransactionLockRequest> lock_requests_current_batch;
	std::uint64_t sent_lock_requests = 0;
	std::uint64_t dtxn_lock_requests = 0;
	std::uint64_t received_lock_responses = 0;
	std::uint64_t received_net_lock_responses = 0;
	std::uint64_t lock_request_done_received = 0;
	std::vector<int> transaction_lengths;
	std::vector<int> transaction_lengths_count;
	static_assert(std::is_same<typename WorkloadType::TransactionType, TransactionType>::value, "Transaction types do not match.");

	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;

	using ProtocolType = Calvin<DatabaseType>;

	using MessageType = CalvinMessage;
	using MessageFactoryType = CalvinMessageFactory;
	using MessageHandlerType = CalvinMessageHandler;

	CalvinExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db, const ContextType &context,
		       std::vector<std::unique_ptr<TransactionType> > &transactions, std::vector<StorageType> &storages,
		       std::atomic<uint32_t> &lock_manager_status, std::atomic<uint32_t> &worker_status, std::atomic<uint32_t> &n_complete_workers,
		       std::atomic<uint32_t> &n_started_workers, std::atomic<uint64_t> &active_txns)
		: Worker(coordinator_id, id)
		, db(db)
		, context(context)
		, transactions(transactions)
		, storages(storages)
		, lock_manager_status(lock_manager_status)
		, worker_status(worker_status)
		, n_complete_workers(n_complete_workers)
		, n_started_workers(n_started_workers)
		, partitioner(coordinator_id, context.coordinator_num, CalvinHelper::string_to_vint(context.replica_group))
		, workload(coordinator_id, db, random, partitioner)
		, n_lock_manager(CalvinHelper::n_lock_manager(partitioner.replica_group_id, id, CalvinHelper::string_to_vint(context.lock_manager)))
		, n_workers(context.worker_num - n_lock_manager)
		, lock_manager_id(CalvinHelper::worker_id_to_lock_manager_id(id, n_lock_manager, n_workers))
		, init_transaction(false)
		, random(id)
		, // make sure each worker has a different seed.
		protocol(db, partitioner)
		, delay(std::make_unique<SameDelay>(coordinator_id, context.coordinator_num, context.delay_time))
		, active_transactions(active_txns)
	{
		transaction_lengths.resize(context.straggler_num_txn_len);
		transaction_lengths_count.resize(context.straggler_num_txn_len);
		transaction_lengths[0] = 10;
		for (size_t i = 1; i < context.straggler_num_txn_len; ++i) {
			transaction_lengths[i] = std::min(context.stragglers_total_wait_time, transaction_lengths[i - 1] * 2);
		}
		for (auto i = 0u; i < context.coordinator_num; i++) {
			messages.emplace_back(std::make_unique<Message>());
			init_message(messages[i].get(), i);
		}

		messageHandlers = MessageHandlerType::get_message_handlers<WorkloadType>();
		CHECK(n_workers > 0 && n_workers % n_lock_manager == 0);

		if (context.log_path != "" && context.wal_group_commit_time != 0) {
                        if (context.lotus_checkpoint == LotusCheckpointScheme::COW_ON_CHECKPOINT_ON_LOGGING_OFF) {
                                logger = context.master_logger;
                        } else {
                                logger = context.slave_loggers[id];
                        }
                } else {
                        if (context.lotus_checkpoint == LotusCheckpointScheme::COW_OFF_CHECKPOINT_OFF_LOGGING_OFF) {
                                logger = context.master_logger;
                        } else {
                                logger = context.master_logger;
                        }
                }
	}

	~CalvinExecutor() = default;

	void clean_up_current_lock_request_batch()
	{
		for (std::size_t i = 0; i < lock_requests_current_batch.size(); ++i) {
			for (std::size_t j = 0; j < lock_requests_current_batch[i].keys.size(); ++j) {
				if (lock_requests_current_batch[i].source_coordinator != (int)coordinator_id) {
					delete[]((char *)lock_requests_current_batch[i].keys[j].get_key());
				}
			}
		}
		lock_requests_current_batch.clear();
	}

	static int64_t next_transaction_id(std::size_t epoch, uint64_t coordinator_id)
	{
		// tid format : epoch id (24bits) | coordinator id(8 bits) | counter (32 bits)
		CHECK(epoch < (1ull << 24));
		CHECK(coordinator_id < (1ull << 8));
		constexpr int epoch_id_offset = 40;
		constexpr int coordinator_id_offset = epoch_id_offset - 8;
		static std::atomic<int64_t> tid_static{ 1 };
		auto tid = tid_static.fetch_add(1);
		CHECK(tid < (1ll << 32));
		return (epoch << epoch_id_offset) | ((int64_t)coordinator_id << coordinator_id_offset) | tid;
	}

	std::size_t epoch = 0;
	void start() override
	{
		LOG(INFO) << "CalvinExecutor " << id << " started. ";

		DCHECK(n_lock_manager == 1);
		for (;;) {
			ExecutorStatus status;
			do {
				status = static_cast<ExecutorStatus>(worker_status.load());

				if (status == ExecutorStatus::EXIT) {
					LOG(INFO) << "CalvinExecutor " << id << " exits. ";
					return;
				}
			} while (status != ExecutorStatus::Analysis);
			// Advance epoch
			epoch += 1;

			lock_request_done_received = 0;
			{
				ScopedTimer t([&, this](uint64_t us) { prepare_stage_time.add(us); });

				if (id < n_lock_manager) {
					// LOG(INFO) << "Analysis active transactions " << active_transactions.load();
				}
				clean_up_current_lock_request_batch();
				n_started_workers.fetch_add(1);
				generate_transactions();
				n_complete_workers.fetch_add(1);

				// wait to LockRequest
				while (static_cast<ExecutorStatus>(worker_status.load()) != ExecutorStatus::LockRequest) {
					std::this_thread::yield();
				}
			}

			{
				ScopedTimer t([&, this](uint64_t us) {
					if (id < n_lock_manager) {
						locking_stage_time.add(us);
					}
				});

				n_started_workers.fetch_add(1);
				// This stage is for lock manager thread only
				if (id < n_lock_manager) {
					// send lock requests to remote node
					// LOG(INFO) << "LockRequest active transactions " << active_transactions.load();
					dtxn_lock_requests = 0;
					send_lock_requests();
					// Wait until we have recevied all the lock requests from all coordinators
					// which is signaled by the CalvinMessage::LOCK_REQUEST_DONE message
					while (lock_request_done_received < partitioner.total_coordinators()) {
						process_request();
					}
				}
				n_complete_workers.fetch_add(1);

				// wait to LockResponse stage
				while (static_cast<ExecutorStatus>(worker_status.load()) != ExecutorStatus::LockResponse) {
					process_request();
				}

				n_started_workers.fetch_add(1);
				// This stage is for lock manager thread only
				if (id < n_lock_manager) {
					// grant locks to transactions
					DCHECK(lock_requests_current_batch.size());
					grant_locks_and_reply();
					wait_for_all_lock_requests_processed();
					// LOG(INFO) << "LockResponse active transactions " << active_transactions.load();
				}
				n_complete_workers.fetch_add(1);

				// wait to Execute

				while (static_cast<ExecutorStatus>(worker_status.load()) != ExecutorStatus::Execute) {
					std::this_thread::yield();
				}
			}

			{
				ScopedTimer t([&, this](uint64_t us) {
					if (id >= n_lock_manager) {
						execution_stage_time.add(us);
					}
				});
				n_started_workers.fetch_add(1);
				// work as lock manager
				if (id < n_lock_manager) {
					// schedule transactions
					schedule_transactions();
					// LOG(INFO) << "Execute active transactions " << active_transactions.load();
				} else {
					// work as executor
					run_transactions();
				}

				n_complete_workers.fetch_add(1);

				// wait to Analysis

				while (static_cast<ExecutorStatus>(worker_status.load()) == ExecutorStatus::Execute) {
					process_request();
				}
			}
		}
	}

	void onExit() override
	{
		LOG(INFO) << "Worker " << this->id << " latency: " << this->percentile.nth(50) << " us (50%) " << this->percentile.nth(75) << " us (75%) "
			  << this->percentile.nth(95) << " us (95%) " << this->percentile.nth(99)
			  << " us (99%). dist txn latency: " << this->dist_latency.nth(50) << " us (50%) " << this->dist_latency.nth(75) << " us (75%) "
			  << this->dist_latency.nth(95) << " us (95%) " << this->dist_latency.nth(99)
			  << " us (99%). local txn latency: " << this->local_latency.nth(50) << " us (50%) " << this->local_latency.nth(75) << " us (75%) "
			  << this->local_latency.nth(95) << " us (95%) " << this->local_latency.nth(99) << " us (99%). "
			  << " scheduling_prepare " << this->prepare_stage_time.nth(50) << " us(50) "
			  << " scheduling_locking " << this->locking_stage_time.nth(50) << " us(50) "
			  << " execution " << this->execution_stage_time.nth(50) << " us(50).\n "
			  << " remote_lock_requests_sent " << this->remote_lock_requests_sent.nth(50) << ". "
			  << " effective conurrency " << this->effective_round_concurrency.nth(50) << ". "
			  << " round conurrency " << this->round_concurrency.nth(50) << ". "
			  << " successful lock requests per batch " << this->succ_lock_reuqests_per_batch.nth(50) << ". "
			  << " lock requests per batch " << this->lock_reuqests_per_batch.nth(50) << ". "
			  << " LOCAL txn stall " << this->local_txn_stall_time_pct.nth(50) << " us, "
			  << " local_work " << this->local_txn_local_work_time_pct.nth(50) << " us, "
			  << " remote_work " << this->local_txn_remote_work_time_pct.nth(50) << " us, "
			  << " commit_work " << this->local_txn_commit_work_time_pct.nth(50) << " us, "
			  << " commit_prepare " << this->local_txn_commit_prepare_time_pct.nth(50) << " us, "
			  << " commit_persistence " << this->local_txn_commit_persistence_time_pct.nth(50) << " us, "
			  << " commit_replication " << this->local_txn_commit_replication_time_pct.nth(50) << " us, "
			  << " commit_write_back " << this->local_txn_commit_write_back_time_pct.nth(50) << " us, "
			  << " commit_release_lock " << this->local_txn_commit_unlock_time_pct.nth(50) << " us \n"
			  << " DIST txn stall " << this->dist_txn_stall_time_pct.nth(50) << " us, "
			  << " local_work " << this->dist_txn_local_work_time_pct.nth(50) << " us, "
			  << " remote_work " << this->dist_txn_remote_work_time_pct.nth(50) << " us, "
			  << " commit_work " << this->dist_txn_commit_work_time_pct.nth(50) << " us, "
			  << " commit_prepare " << this->dist_txn_commit_prepare_time_pct.nth(50) << " us, "
			  << " commit_persistence " << this->dist_txn_commit_persistence_time_pct.nth(50) << " us, "
			  << " commit_replication " << this->local_txn_commit_replication_time_pct.nth(50) << " us, "
			  << " commit_write_back " << this->dist_txn_commit_write_back_time_pct.nth(50) << " us, "
			  << " commit_release_lock " << this->dist_txn_commit_unlock_time_pct.nth(50) << " us \n";
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

	void flush_messages()
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

	void init_message(Message *message, std::size_t dest_node_id)
	{
		message->set_source_node_id(coordinator_id);
		message->set_dest_node_id(dest_node_id);
		message->set_worker_id(id);
	}

	std::size_t get_partition_id()
	{
		std::size_t partition_id;

		auto partition_num_per_node = context.partition_num / context.coordinator_num;
		partition_id = random.uniform_dist(0, partition_num_per_node - 1) * context.coordinator_num + coordinator_id;
		CHECK(partitioner.has_master_partition(partition_id));
		return partition_id;
	}

	std::unordered_map<int64_t, int> tid_to_txn_idx;
	void generate_transactions()
	{
		// if (init_transaction && active_transactions.load()) {
		//   for (auto i = id; i < transactions.size(); i += context.worker_num) {
		//     if (!transactions[i]->processed && transactions[i]->abort_no_retry == false) {
		//       transactions[i]->reset();
		//       // re-prepare unprocessed transaction
		//       prepare_transaction(*transactions[i]);
		//     }
		//   }
		//   return;
		// }
		init_transaction = true;
		tid_to_txn_idx.clear();
		std::string txn_command_data;

		for (auto i = id; i < transactions.size(); i += context.worker_num) {
			if (transactions[i] != nullptr && transactions[i]->abort_no_retry == false && !transactions[i]->processed) {
				transactions[i]->reset();
				prepare_transaction(*transactions[i]);
				continue;
			}
			// generate transaction
			auto partition_id = get_partition_id();
			transactions[i] = workload.next_transaction(context, partition_id, this->id);
			transactions[i]->transaction_id = next_transaction_id(epoch, this->coordinator_id);
			if (this->context.stragglers_per_batch) {
				auto total_batch_size = this->partitioner.num_coordinator_for_one_replica() * this->context.batch_size;
				auto v = this->random.uniform_dist(1, total_batch_size);
				if (v <= (uint64_t)this->context.stragglers_per_batch) {
					transactions[i]->straggler_wait_time = this->context.stragglers_total_wait_time / this->context.stragglers_per_batch;
				}
			}
			if (context.straggler_zipf_factor > 0) {
				int length_type = star::Zipf::globalZipfForStraggler().value(random.next_double());
				transactions[i]->straggler_wait_time = transaction_lengths[length_type];
				transaction_lengths_count[length_type]++;
			}
			active_transactions.fetch_add(1);
			txn_command_data += transactions[i]->serialize(0);
			transactions[i]->set_id(transactions[i]->transaction_id);
			prepare_transaction(*transactions[i]);
		}

		this->logger->write(txn_command_data.data(), txn_command_data.size(), true);
	}

	void prepare_transaction(TransactionType &txn)
	{
		setup_prepare_handlers(txn);
		// run execute to prepare read/write set
		auto result = txn.execute(id);
		if (result == TransactionResult::ABORT_NORETRY) {
			active_transactions.fetch_sub(1);
			txn.abort_no_retry = true;
		}

		if (context.calvin_same_batch) {
			txn.save_read_count();
		}

		analyze_active_coordinator(txn);

		// setup handlers for execution
		setup_execute_handlers(txn);
		txn.execution_phase = true;
	}

	void analyze_active_coordinator(TransactionType &transaction)
	{
		// assuming no blind write
		auto &readSet = transaction.readSet;
		auto &active_coordinators = transaction.active_coordinators;
		active_coordinators = std::vector<bool>(partitioner.total_coordinators(), false);
		auto &lock_request_for_coordinators = transaction.lock_request_for_coordinators;
		CHECK(readSet.empty() == false);
		for (auto i = 0u; i < readSet.size(); i++) {
			auto &readkey = readSet[i];

			auto partitionID = readkey.get_partition_id();

			auto target_coordinator = partitioner.master_coordinator(partitionID);
			if (lock_request_for_coordinators[target_coordinator].empty()) {
				lock_request_for_coordinators[target_coordinator] = { (int)this->coordinator_id, transaction.transaction_id, {}, {} };
			}
			lock_request_for_coordinators[target_coordinator].add_key(readkey);
		}
	}

	std::size_t new_lock_request_message(Message &message, CalvinTransaction::TransactionLockRequest &lock_request, int target_coordinator_id)
	{
		/*
		 * The structure of a lock request: (tid, source_node, n_keys,
		  [table_id, partition_id, read_or_write, key_size, key_bytes] * n_keys)
		 */
		auto message_size =
			MessagePiece::get_header_size() + sizeof(lock_request.tid) + sizeof(lock_request.source_coordinator) + sizeof(lock_request.keys.size());

		auto message_piece_header =
			MessagePiece::construct_message_piece_header(static_cast<uint32_t>(CalvinMessage::LOCK_REQUEST), message_size, 0, 0);

		Encoder encoder(message.data);
		size_t start_off = encoder.size();
		encoder << message_piece_header;
		encoder << lock_request.tid << lock_request.source_coordinator << lock_request.keys.size();
		for (size_t i = 0; i < lock_request.keys.size(); ++i) {
			auto table_id = lock_request.table_ids[i];
			auto partition_id = lock_request.partition_ids[i];
			ITable *table = this->db.find_table(table_id, partition_id);
			auto key_size = table->key_size();
			bool read_or_write = lock_request.read_writes[i];
			auto key = lock_request.keys[i];

			encoder << table_id << partition_id << read_or_write << key_size;
			encoder.write_n_bytes(key.get_key(), key_size);

			message_size += key_size + sizeof(table_id) + sizeof(partition_id) + sizeof(key_size) + sizeof(read_or_write);
		}

		message_piece_header = MessagePiece::construct_message_piece_header(static_cast<uint32_t>(CalvinMessage::LOCK_REQUEST), message_size, 0, 0);
		encoder.replace_bytes_range(start_off, (void *)&message_piece_header, sizeof(message_piece_header));
		message.flush();
		// LOG(INFO) << " worker " <<  id << " sent a lock request of " << lock_request.keys.size() << " keys from source coordinator " <<
		// coordinator_id << " tid " << lock_request.tid << " to coordinator " << target_coordinator_id;

		return message_size;
	}

	std::size_t new_lock_request_done_message(Message &message, int source_coordinator)
	{
		/*
		 * The structure of a lock request: (source_coordinator)
		 */
		auto message_size = MessagePiece::get_header_size() + sizeof(source_coordinator);

		auto message_piece_header =
			MessagePiece::construct_message_piece_header(static_cast<uint32_t>(CalvinMessage::LOCK_REQUEST_DONE), message_size, 0, 0);

		Encoder encoder(message.data);
		size_t start_off = encoder.size();
		encoder << message_piece_header;
		encoder << source_coordinator;
		message.flush();

		return message_size;
	}

	std::size_t new_lock_response_message(Message &message, int64_t tid, bool success)
	{
		/*
		 * The structure of a lock response: (tid, succees)
		 */

		auto message_size = MessagePiece::get_header_size() + sizeof(tid) + sizeof(success);
		auto message_piece_header =
			MessagePiece::construct_message_piece_header(static_cast<uint32_t>(CalvinMessage::LOCK_RESPONSE), message_size, 0, 0);

		Encoder encoder(message.data);
		encoder << message_piece_header << tid << success;
		message.flush();
		message.set_gen_time(Time::now());
		return message_size;
	}

	void send_lock_requests()
	{
		std::size_t request_id = 0;
		auto sent_lock_requests_old = sent_lock_requests;
		for (auto i = 0u; i < transactions.size(); i++) {
			tid_to_txn_idx[transactions[i]->transaction_id] = i;
			// do not grant locks to abort no retry transaction
			if (transactions[i]->abort_no_retry)
				continue;
			if (transactions[i]->processed)
				continue;
			auto &txn = transactions[i];
			for (std::size_t j = 0; j < this->context.coordinator_num; ++j) {
				if (j == coordinator_id) {
					if (!txn->lock_request_for_coordinators[j].empty()) {
						lock_requests_current_batch.push_back(txn->lock_request_for_coordinators[j]);
						sent_lock_requests++;
					}
					continue;
				}
				if (txn->lock_request_for_coordinators[j].empty())
					continue;
				auto sz = new_lock_request_message(*messages[j], txn->lock_request_for_coordinators[j], j);
				txn->network_size.fetch_add(sz);
				txn->distributed_transaction = true;
				sent_lock_requests++;
				dtxn_lock_requests++;
			}
			flush_messages();
		}
		for (std::size_t j = 0; j < this->context.coordinator_num; ++j) {
			if (j == coordinator_id) {
				lock_request_done_received++;
				continue;
			}
			auto sz = new_lock_request_done_message(*messages[j], coordinator_id);
		}
		flush_messages();
		remote_lock_requests_sent.add(dtxn_lock_requests);
	}

	void collect_vote_for_txn(int64_t tid, bool success)
	{
		DCHECK(id == 0); // Only lock manager is allowed to call this.
		DCHECK(tid_to_txn_idx.count(tid));
		auto idx = tid_to_txn_idx[tid];
		DCHECK(transactions[idx]->processed == false);
		transactions[idx]->votes.push_back(success);
		received_lock_responses++;
	}

	void send_lock_responses(const std::vector<bool> &all_locks_obtained)
	{
		DCHECK(all_locks_obtained.size() == lock_requests_current_batch.size());
		for (size_t i = 0; i < lock_requests_current_batch.size(); ++i) {
			auto &req = lock_requests_current_batch[i];
			if (req.source_coordinator == (int)coordinator_id) {
				collect_vote_for_txn(req.tid, all_locks_obtained[i]);
			} else {
				auto sz = new_lock_response_message(*messages[req.source_coordinator], req.tid, all_locks_obtained[i]);
				flush_messages();
			}
		}
	}

	void grant_locks_and_reply()
	{
		std::size_t request_id = 0;

		// Sort the transactions based on tid to obtain the global serial order.
		std::sort(lock_requests_current_batch.begin(), lock_requests_current_batch.end(),
			  [](const TransactionType::TransactionLockRequest &lhs, const TransactionType::TransactionLockRequest &rhs) {
				  return lhs.tid < rhs.tid;
			  });

		// Grant the locks serially to the transactions.
		// If one transaction can not get all its required locks,
		// it will be scheduled for the next bacth.

		std::vector<bool> all_locks_obtained(lock_requests_current_batch.size());
		for (size_t i = 0; i < lock_requests_current_batch.size(); ++i) {
			if (i > 0)
				DCHECK(lock_requests_current_batch[i].tid > lock_requests_current_batch[i - 1].tid);
			auto &req = lock_requests_current_batch[i];
			bool can_obtain_all_the_locks = true;
			for (size_t j = 0; j < req.keys.size() && can_obtain_all_the_locks; ++j) {
				auto key = req.keys[j];
				auto partition_id = key.get_partition_id();
				auto table_id = key.get_table_id();
				auto table = db.find_table(table_id, partition_id);
				const std::atomic<uint64_t> &meta = *table->search_metadata(key.get_key());
				if (key.get_read_lock_bit()) {
					if (CalvinHelper::is_write_locked(meta.load())) {
						can_obtain_all_the_locks = false;
					}
				} else {
					if (CalvinHelper::is_write_locked(meta.load()) || CalvinHelper::is_read_locked(meta.load())) {
						can_obtain_all_the_locks = false;
					}
				}
			}

			all_locks_obtained[i] = can_obtain_all_the_locks;
			if (can_obtain_all_the_locks) {
				for (size_t j = 0; j < req.keys.size(); ++j) {
					auto key = req.keys[j];
					auto partition_id = key.get_partition_id();
					auto table_id = key.get_table_id();
					auto table = db.find_table(table_id, partition_id);
					std::atomic<uint64_t> &meta = *table->search_metadata(key.get_key());

					if (key.get_read_lock_bit()) {
						DCHECK(CalvinHelper::is_write_locked(meta.load()) == false);
						CalvinHelper::read_lock(meta);
						CHECK(CalvinHelper::is_read_locked(meta.load()));
					} else {
						DCHECK(CalvinHelper::is_read_locked(meta.load()) == false);
						if (CalvinHelper::is_write_locked(meta.load())) // Could have same keys from this transaction, one lock is
												// enough
							continue;
						CalvinHelper::write_lock(meta);
						CHECK(CalvinHelper::is_write_locked(meta.load()));
					}
				}
			}
		}

		std::size_t succ_lock_rqs = 0;
		// Now we have figured out which transactions succeeded or failed, release all the locks we obtained.
		for (size_t i = 0; i < lock_requests_current_batch.size(); ++i) {
			if (i > 0)
				DCHECK(lock_requests_current_batch[i].tid > lock_requests_current_batch[i - 1].tid);
			auto &req = lock_requests_current_batch[i];
			bool can_obtain_all_the_locks = all_locks_obtained[i];

			if (can_obtain_all_the_locks) {
				succ_lock_rqs++;
				for (size_t j = 0; j < req.keys.size() && can_obtain_all_the_locks; ++j) {
					auto key = req.keys[j];
					auto partition_id = key.get_partition_id();
					auto table_id = key.get_table_id();
					auto table = db.find_table(table_id, partition_id);
					std::atomic<uint64_t> &meta = *table->search_metadata(key.get_key());

					if (key.get_read_lock_bit()) {
						DCHECK(CalvinHelper::is_write_locked(meta.load()) == false);
						CalvinHelper::read_lock_release(meta);
					} else {
						DCHECK(CalvinHelper::is_read_locked(meta.load()) == false);
						if (CalvinHelper::is_write_locked(meta.load()))
							CalvinHelper::write_lock_release(meta);
					}
				}
			}
		}

		// Send reply to the original coordinator
		send_lock_responses(all_locks_obtained);
		this->succ_lock_reuqests_per_batch.add(succ_lock_rqs);
		this->lock_reuqests_per_batch.add(all_locks_obtained.size());
	}

	void wait_for_all_lock_requests_processed()
	{
		while (received_lock_responses < sent_lock_requests) {
			process_request();
		}
		DCHECK(received_lock_responses == sent_lock_requests);
	}

	std::size_t need_vote_count(TransactionType *txn)
	{
		std::size_t cnt = 0;
		for (size_t i = 0; i < txn->lock_request_for_coordinators.size(); ++i)
			cnt += txn->lock_request_for_coordinators[i].empty() == false;
		return cnt;
	}

	std::size_t good_vote_count(TransactionType *txn)
	{
		std::size_t cnt = 0;
		for (size_t i = 0; i < txn->votes.size(); ++i)
			cnt += txn->votes[i];
		return cnt;
	}

	void schedule_transactions()
	{
		// grant locks, once all locks are acquired, assign the transaction to
		// a worker thread in a round-robin manner.

		std::size_t request_id = 0;
		std::size_t good_txns = 0;
		for (auto i = 0u; i < transactions.size(); i++) {
			// do not grant locks to abort no retry transaction
			if (transactions[i]->processed)
				continue;
			if (transactions[i]->abort_no_retry)
				continue;
			bool is_single_partition = transactions[i]->is_single_partition();
			auto vote_got = transactions[i]->votes.size();
			auto good_vote = good_vote_count(transactions[i].get());
			auto vote_need = need_vote_count(transactions[i].get());
			DCHECK(transactions[i]->votes.size() == vote_need);
			if (good_vote != vote_need) {
				transactions[i]->votes.clear();
				continue;
			}
			good_txns++;
			bool grant_lock = false;
			auto worker = get_available_worker(request_id++);
			all_executors[worker]->transaction_queue.push(transactions[i].get());
			// only count once
			n_commit.fetch_add(1);
		}
		set_lock_manager_bit(id);
		effective_round_concurrency.add(good_txns);
		round_concurrency.add(transactions.size());
	}

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

	void run_transactions()
	{
		size_t cc = 0;
		std::vector<TransactionType *> outbound_transactions;
		while (!get_lock_manager_bit(lock_manager_id) || !transaction_queue.empty()) {
			if (transaction_queue.empty()) {
				process_request();
				continue;
			}
			cc++;
			TransactionType *transaction = transaction_queue.front();
			bool ok = transaction_queue.pop();
			transaction->async = true;
			DCHECK(ok);
			outbound_transactions.push_back(transaction);
			auto ltc = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - transaction->startTime).count();
			transaction->set_stall_time(ltc);
			active_txns[transaction->transaction_id] = transaction;
			auto result = transaction->execute(id);
			n_network_size.fetch_add(transaction->network_size.load());
		}

		for (size_t i = 0; i < outbound_transactions.size(); ++i) {
			auto transaction = outbound_transactions[i];
			while (transaction->remote_read) {
				process_request();
			}
			auto result = transaction->execute(id);
			if (result == TransactionResult::READY_TO_COMMIT) {
				bool commit;
				{
					ScopedTimer t([&, this](uint64_t us) {
						if (commit) {
							transaction->record_commit_work_time(us);
						} else {
							auto ltc = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() -
															 transaction->startTime)
									   .count();
							transaction->set_stall_time(ltc);
						}
					});
					{
						ScopedTimer t([&, this](uint64_t us) { transaction->record_commit_write_back_time(us); });
						commit = protocol.commit(messages, *transaction, lock_manager_id, n_lock_manager,
									 partitioner.replica_group_size);
						flush_messages();
					}
					auto latency =
						std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - transaction->startTime)
							.count();
					this->percentile.add(latency);
					if (transaction->distributed_transaction) {
						this->dist_latency.add(latency);
					} else {
						this->local_latency.add(latency);
					}
					record_txn_breakdown_stats(*transaction);
				}
			} else if (result == TransactionResult::ABORT) {
				// non-active transactions, release lock
				protocol.abort(messages, *transaction, lock_manager_id, n_lock_manager, partitioner.replica_group_size);
			} else {
				CHECK(false) << "abort no retry transaction should not be scheduled.";
			}
		}

		// Wait for all writes to be applied to remote nodes
		for (size_t i = 0; i < outbound_transactions.size(); ++i) {
			auto transaction = outbound_transactions[i];
			while (outbound_transactions[i]->remote_write) {
				process_request();
			}
			transaction->processed = true;
			active_transactions.fetch_sub(1);
		}

		this->effective_round_concurrency.add(cc);
	}

	void setup_execute_handlers(TransactionType &txn)
	{
		txn.read_handler = [this, &txn](std::size_t worker_id, std::size_t table_id, std::size_t partition_id, std::size_t id, uint32_t key_offset,
						const void *key, void *value) {
			auto *worker = this->all_executors[worker_id];
			ITable *table = worker->db.find_table(table_id, partition_id);
			if (worker->partitioner.has_master_partition(partition_id)) {
				CalvinHelper::read(table->search(key), value, table->value_size());

				txn.local_read.fetch_add(-1);
			} else {
				auto coordinator_id = partitioner.master_coordinator(partition_id);
				worker->messages[coordinator_id]->set_transaction_id(txn.transaction_id);
				auto sz = MessageFactoryType::new_read_message(*worker->messages[coordinator_id], *table, id, key_offset, key);
				txn.network_size.fetch_add(sz);
				txn.distributed_transaction = true;
			}
		};

		txn.setup_process_requests_in_execution_phase(n_lock_manager, n_workers, partitioner.replica_group_size);
		txn.remote_request_handler = [this](std::size_t worker_id) {
			auto *worker = this->all_executors[worker_id];
			return worker->process_request();
		};
		txn.get_table = [this](std::size_t tableId, std::size_t partitionId) { return this->db.find_table(tableId, partitionId); };
		txn.message_flusher = [this](std::size_t worker_id) {
			auto *worker = this->all_executors[worker_id];
			worker->flush_messages();
		};
	};

	void setup_prepare_handlers(TransactionType &txn)
	{
		txn.local_index_read_handler = [this](std::size_t table_id, std::size_t partition_id, const void *key, void *value) {
			ITable *table = this->db.find_table(table_id, partition_id);
			CalvinHelper::read(table->search(key), value, table->value_size());
		};
		txn.setup_process_requests_in_prepare_phase();
	};

	void set_all_executors(const std::vector<CalvinExecutor *> &executors)
	{
		all_executors = executors;
	}

	std::size_t get_available_worker(std::size_t request_id)
	{
		// assume there are n lock managers and m workers
		// 0, 1, .. n-1 are lock managers
		// n, n + 1, .., n + m -1 are workers

		// the first lock managers assign transactions to n, .. , n + m/n - 1

		auto start_worker_id = n_lock_manager + n_workers / n_lock_manager * id;
		auto len = n_workers / n_lock_manager;
		return request_id % len + start_worker_id;
	}

	void set_lock_manager_bit(int id)
	{
		uint32_t old_value, new_value;
		do {
			old_value = lock_manager_status.load();
			DCHECK(((old_value >> id) & 1) == 0);
			new_value = old_value | (1 << id);
		} while (!lock_manager_status.compare_exchange_weak(old_value, new_value));
	}

	bool get_lock_manager_bit(int id)
	{
		return (lock_manager_status.load() >> id) & 1;
	}

	std::size_t process_request()
	{
		std::size_t size = 0;

		while (!in_queue.empty()) {
			std::unique_ptr<Message> message(in_queue.front());
			bool ok = in_queue.pop();
			CHECK(ok);

			for (auto it = message->begin(); it != message->end(); it++) {
				auto tid = message->get_transaction_id();
				TransactionType *this_transaction = nullptr;
				if (active_txns.count(tid) > 0) {
					this_transaction = active_txns[tid];
				}
				MessagePiece messagePiece = *it;
				auto type = messagePiece.get_message_type();
				DCHECK(type < messageHandlers.size());
				ITable *table = db.find_table(messagePiece.get_table_id(), messagePiece.get_partition_id());
				messageHandlers[type](*message, messagePiece, *messages[message->get_source_node_id()], *table, this_transaction, this);
			}

			size += message->get_message_count();
			flush_messages();
		}
		return size;
	}

	DatabaseType &db;
	ContextType context;
	std::vector<std::unique_ptr<TransactionType> > &transactions;
	std::vector<StorageType> &storages;
	std::atomic<uint32_t> &lock_manager_status, &worker_status;
	std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
	CalvinPartitioner partitioner;
	WorkloadType workload;
	std::size_t n_lock_manager, n_workers;
	std::size_t lock_manager_id;
	bool init_transaction;
	RandomType random;
	ProtocolType protocol;
	std::unique_ptr<Delay> delay;
	std::atomic<uint64_t> &active_transactions;
	WALLogger *logger = nullptr;
	std::vector<std::unique_ptr<Message> > messages;
	std::vector<std::function<void(Message &, MessagePiece, Message &, ITable &, TransactionType *, CalvinExecutor<Workload> *)> > messageHandlers;
	LockfreeQueue<Message *> in_queue, out_queue;
	LockfreeQueue<TransactionType *> transaction_queue;
	std::vector<CalvinExecutor *> all_executors;
	std::unordered_map<long long, TransactionType *> active_txns;
	Percentile<int64_t> percentile, dist_latency, local_latency, commit_latency;
	Percentile<int64_t> effective_round_concurrency;
	Percentile<int64_t> round_concurrency;
	Percentile<int64_t> remote_lock_requests_sent;
	Percentile<int64_t> succ_lock_reuqests_per_batch;
	Percentile<int64_t> lock_reuqests_per_batch;
	Percentile<int64_t> prepare_stage_time;
	Percentile<int64_t> locking_stage_time;
	Percentile<int64_t> execution_stage_time;
	Percentile<uint64_t> local_txn_stall_time_pct, local_txn_commit_work_time_pct, local_txn_commit_persistence_time_pct, local_txn_commit_prepare_time_pct,
		local_txn_commit_replication_time_pct, local_txn_commit_write_back_time_pct, local_txn_commit_unlock_time_pct, local_txn_local_work_time_pct,
		local_txn_remote_work_time_pct;
	Percentile<uint64_t> dist_txn_stall_time_pct, dist_txn_commit_work_time_pct, dist_txn_commit_persistence_time_pct, dist_txn_commit_prepare_time_pct,
		dist_txn_commit_write_back_time_pct, dist_txn_commit_unlock_time_pct, dist_txn_local_work_time_pct, dist_txn_commit_replication_time_pct,
		dist_txn_remote_work_time_pct;
};
} // namespace star