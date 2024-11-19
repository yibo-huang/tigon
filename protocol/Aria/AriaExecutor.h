//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "core/Partitioner.h"

#include "common/Percentile.h"
#include "core/Delay.h"
#include "core/Worker.h"
#include "glog/logging.h"

#include "protocol/Aria/Aria.h"
#include "protocol/Aria/AriaHelper.h"
#include "protocol/Aria/AriaMessage.h"

#include <chrono>
#include <thread>

namespace star
{

template <class Workload> class AriaExecutor : public Worker {
    public:
	using WorkloadType = Workload;
	using DatabaseType = typename WorkloadType::DatabaseType;
	using StorageType = typename WorkloadType::StorageType;

	using TransactionType = AriaTransaction;
	static_assert(std::is_same<typename WorkloadType::TransactionType, TransactionType>::value, "Transaction types do not match.");

	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;

	using ProtocolType = Aria<DatabaseType>;

	using MessageType = AriaMessage;
	using MessageFactoryType = AriaMessageFactory;
	using MessageHandlerType = AriaMessageHandler;
	std::vector<int> transaction_lengths;
	std::vector<int> transaction_lengths_count;
	AriaExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db, const ContextType &context,
		     std::vector<std::unique_ptr<TransactionType> > &transactions, std::vector<StorageType> &storages, std::atomic<uint32_t> &epoch,
		     std::atomic<uint32_t> &worker_status, std::atomic<uint32_t> &total_abort, std::atomic<uint32_t> &n_complete_workers,
		     std::atomic<uint32_t> &n_started_workers)
		: Worker(coordinator_id, id)
		, db(db)
		, context(context)
		, transactions(transactions)
		, storages(storages)
		, epoch(epoch)
		, worker_status(worker_status)
		, total_abort(total_abort)
		, n_complete_workers(n_complete_workers)
		, n_started_workers(n_started_workers)
		, partitioner(PartitionerFactory::create_partitioner(context.partitioner, coordinator_id, context.coordinator_num))
		, workload(coordinator_id, db, random, *partitioner)
		, random(reinterpret_cast<uint64_t>(this))
		, protocol(db, context, *partitioner)
		, delay(std::make_unique<SameDelay>(coordinator_id, context.coordinator_num, context.delay_time))
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

		messageHandlers = MessageHandlerType::get_message_handlers();
	}

	~AriaExecutor() = default;

	void start() override
	{
		LOG(INFO) << "AriaExecutor " << id << " started. ";

		for (;;) {
			{
				ExecutorStatus status;
				do {
					status = static_cast<ExecutorStatus>(worker_status.load());

					if (status == ExecutorStatus::EXIT) {
						LOG(INFO) << "AriaExecutor " << id << " exits. ";
						return;
					}
				} while (status != ExecutorStatus::Aria_COLLECT_XACT);

				n_started_workers.fetch_add(1);
				generate_transactions();
				n_complete_workers.fetch_add(1);
				ScopedTimer t2([&, this](uint64_t us) { this->sequencing_time.add(us); });
				while (static_cast<ExecutorStatus>(worker_status.load()) == ExecutorStatus::Aria_COLLECT_XACT) {
					process_request();
				}
				process_request();
				n_complete_workers.fetch_add(1);

				// wait till Aria_READ
				while (static_cast<ExecutorStatus>(worker_status.load()) != ExecutorStatus::Aria_READ) {
					std::this_thread::yield();
				}
			}

			{
				ScopedTimer t2([&, this](uint64_t us) { this->execution_time.add(us); });
				n_started_workers.fetch_add(1);
				read_snapshot();
				n_complete_workers.fetch_add(1);
				// wait to Aria_READ
				while (static_cast<ExecutorStatus>(worker_status.load()) == ExecutorStatus::Aria_READ) {
					process_request();
				}
				process_request();
				n_complete_workers.fetch_add(1);

				// wait till Aria_COMMIT
				while (static_cast<ExecutorStatus>(worker_status.load()) != ExecutorStatus::Aria_COMMIT) {
					std::this_thread::yield();
				}
			}
			n_started_workers.fetch_add(1);
			{
				ScopedTimer t2([&, this](uint64_t us) { this->commit_time.add(us); });
				commit_transactions();
				n_complete_workers.fetch_add(1);
				// wait to Aria_COMMIT
				while (static_cast<ExecutorStatus>(worker_status.load()) == ExecutorStatus::Aria_COMMIT) {
					process_request();
				}
			}
			process_request();
			n_complete_workers.fetch_add(1);
		}
	}

	std::size_t get_partition_id()
	{
		std::size_t partition_id;

		CHECK(context.partition_num % context.coordinator_num == 0);

		auto partition_num_per_node = context.partition_num / context.coordinator_num;
		partition_id = random.uniform_dist(0, partition_num_per_node - 1) * context.coordinator_num + coordinator_id;
		CHECK(partitioner->has_master_partition(partition_id));
		return partition_id;
	}

	void generate_transactions()
	{
		auto n_abort = total_abort.load();
		for (std::size_t i = id; i < transactions.size(); i += context.worker_num) {
			// if null, generate a new transaction, on this node.
			// else only reset the query

			if (transactions[i] == nullptr || i >= n_abort) {
				auto partition_id = get_partition_id();
				transactions[i] = workload.next_transaction(context, partition_id, this->id);
				auto total_batch_size = context.coordinator_num * context.batch_size;
				if (context.stragglers_per_batch) {
					auto v = random.uniform_dist(1, total_batch_size);
					if (v <= (uint64_t)context.stragglers_per_batch) {
						transactions[i]->straggler_wait_time = context.stragglers_total_wait_time / context.stragglers_per_batch;
					}
				}
				if (context.straggler_zipf_factor > 0) {
					int length_type = star::Zipf::globalZipfForStraggler().value(random.next_double());
					transactions[i]->straggler_wait_time = transaction_lengths[length_type];
					transaction_lengths_count[length_type]++;
				}
			} else {
				transactions[i]->reset();
				transactions[i]->aria_aborted = true;
			}
		}
	}

	void read_snapshot()
	{
		// load epoch
		auto cur_epoch = epoch.load();
		std::size_t count = 0;
		std::string txn_command_data;

		for (auto i = id; i < transactions.size(); i += context.worker_num) {
			process_request();

			transactions[i]->set_epoch(cur_epoch);
			transactions[i]->set_id(i * context.coordinator_num + coordinator_id + 1); // tid starts from 1
			transactions[i]->set_tid_offset(i);
			transactions[i]->execution_phase = false;
			setupHandlers(*transactions[i]);

			count++;
			auto ltc = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - transactions[i]->startTime).count();
			transactions[i]->set_stall_time(ltc);
			// run transactions
			auto result = transactions[i]->execute(id);
			n_network_size.fetch_add(transactions[i]->network_size);
			if (result == TransactionResult::ABORT_NORETRY) {
				transactions[i]->abort_no_retry = true;
			}

			if (count % context.batch_flush == 0) {
				flush_messages();
			}
		}
		flush_messages();

		// reserve
		count = 0;
		for (auto i = id; i < transactions.size(); i += context.worker_num) {
			if (transactions[i]->abort_no_retry) {
				continue;
			}

			count++;

			// wait till all reads are processed
			while (transactions[i]->pendingResponses > 0) {
				process_request();
			}

			transactions[i]->execution_phase = true;
			// fill in writes in write set
			transactions[i]->execute(id);

			// start reservation
			reserve_transaction(*transactions[i]);
			if (count % context.batch_flush == 0) {
				flush_messages();
			}
		}
		flush_messages();
	}

	void reserve_transaction(TransactionType &txn)
	{
		if (context.aria_read_only_optmization && txn.is_read_only()) {
			return;
		}

		std::vector<AriaRWKey> &readSet = txn.readSet;
		std::vector<AriaRWKey> &writeSet = txn.writeSet;

		// reserve reads;
		for (std::size_t i = 0u; i < readSet.size(); i++) {
			AriaRWKey &readKey = readSet[i];
			if (readKey.get_local_index_read_bit()) {
				continue;
			}

			auto tableId = readKey.get_table_id();
			auto partitionId = readKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);
			if (partitioner->has_master_partition(partitionId)) {
				std::atomic<uint64_t> &tid = AriaHelper::get_metadata(table, readKey);
				readKey.set_tid(&tid);
				AriaHelper::reserve_read(tid, txn.epoch, txn.id);
			} else {
				auto coordinatorID = this->partitioner->master_coordinator(partitionId);
				txn.network_size += MessageFactoryType::new_reserve_message(*(this->messages[coordinatorID]), *table, txn.id, readKey.get_key(),
											    txn.epoch, false);
			}
		}

		// reserve writes
		for (std::size_t i = 0u; i < writeSet.size(); i++) {
			AriaRWKey &writeKey = writeSet[i];
			auto tableId = writeKey.get_table_id();
			auto partitionId = writeKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);
			if (partitioner->has_master_partition(partitionId)) {
				std::atomic<uint64_t> &tid = AriaHelper::get_metadata(table, writeKey);
				writeKey.set_tid(&tid);
				AriaHelper::reserve_write(tid, txn.epoch, txn.id);
			} else {
				auto coordinatorID = this->partitioner->master_coordinator(partitionId);
				txn.network_size += MessageFactoryType::new_reserve_message(*(this->messages[coordinatorID]), *table, txn.id,
											    writeKey.get_key(), txn.epoch, true);
			}
		}
	}

	void analyze_dependency(TransactionType &txn)
	{
		if (context.aria_read_only_optmization && txn.is_read_only()) {
			return;
		}

		const std::vector<AriaRWKey> &readSet = txn.readSet;
		const std::vector<AriaRWKey> &writeSet = txn.writeSet;

		// analyze raw

		for (std::size_t i = 0u; i < readSet.size(); i++) {
			const AriaRWKey &readKey = readSet[i];
			if (readKey.get_local_index_read_bit()) {
				continue;
			}

			auto tableId = readKey.get_table_id();
			auto partitionId = readKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);

			if (partitioner->has_master_partition(partitionId)) {
				uint64_t tid = AriaHelper::get_metadata(table, readKey).load();
				uint64_t epoch = AriaHelper::get_epoch(tid);
				uint64_t wts = AriaHelper::get_wts(tid);
				DCHECK(epoch == txn.epoch);
				if (epoch == txn.epoch && wts < txn.id && wts != 0) {
					txn.raw = true;
					break;
				}
			} else {
				auto coordinatorID = this->partitioner->master_coordinator(partitionId);
				txn.network_size += MessageFactoryType::new_check_message(*(this->messages[coordinatorID]), *table, txn.id, txn.tid_offset,
											  readKey.get_key(), txn.epoch, false);
				txn.pendingResponses++;
			}
		}

		// analyze war and waw

		for (std::size_t i = 0u; i < writeSet.size(); i++) {
			const AriaRWKey &writeKey = writeSet[i];

			auto tableId = writeKey.get_table_id();
			auto partitionId = writeKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);

			if (partitioner->has_master_partition(partitionId)) {
				uint64_t tid = AriaHelper::get_metadata(table, writeKey).load();
				uint64_t epoch = AriaHelper::get_epoch(tid);
				uint64_t rts = AriaHelper::get_rts(tid);
				uint64_t wts = AriaHelper::get_wts(tid);
				DCHECK(epoch == txn.epoch);
				if (epoch == txn.epoch && rts < txn.id && rts != 0) {
					txn.war = true;
				}
				if (epoch == txn.epoch && wts < txn.id && wts != 0) {
					txn.waw = true;
				}
			} else {
				auto coordinatorID = this->partitioner->master_coordinator(partitionId);
				txn.network_size += MessageFactoryType::new_check_message(*(this->messages[coordinatorID]), *table, txn.id, txn.tid_offset,
											  writeKey.get_key(), txn.epoch, true);
				txn.pendingResponses++;
			}
		}
	}

	void commit_transactions()
	{
		std::size_t count = 0;
		for (auto i = id; i < transactions.size(); i += context.worker_num) {
			if (transactions[i]->abort_no_retry) {
				continue;
			}

			count++;

			analyze_dependency(*transactions[i]);
			if (count % context.batch_flush == 0) {
				flush_messages();
			}
		}
		flush_messages();
		size_t concur = 0;
		size_t effective_concur = 0;
		count = 0;
		for (auto i = id; i < transactions.size(); i += context.worker_num) {
			++concur;
			if (transactions[i]->abort_no_retry) {
				n_abort_no_retry.fetch_add(1);
				continue;
			}
			count++;

			// wait till all checks are processed
			while (transactions[i]->pendingResponses > 0) {
				process_request();
			}

			if (context.aria_read_only_optmization && transactions[i]->is_read_only()) {
				n_commit.fetch_add(1);
				effective_concur++;
				auto latency =
					std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - transactions[i]->startTime)
						.count();
				percentile.add(latency);
				record_txn_breakdown_stats(*transactions[i]);
				continue;
			}

			if (transactions[i]->waw) {
				protocol.abort(*transactions[i], messages);
				n_abort_lock.fetch_add(1);
				continue;
			}

			if (context.aria_snapshot_isolation) {
				protocol.commit(*transactions[i], messages);
				effective_concur++;
				n_commit.fetch_add(1);
				auto latency =
					std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - transactions[i]->startTime)
						.count();
				percentile.add(latency);
				if (transactions[i]->distributed_transaction) {
					this->dist_latency.add(latency);
				} else {
					this->local_latency.add(latency);
				}
				record_txn_breakdown_stats(*transactions[i]);
			} else {
				if (context.aria_reordering_optmization) {
					if (transactions[i]->war == false || transactions[i]->raw == false) {
						effective_concur++;
						protocol.commit(*transactions[i], messages);
						n_commit.fetch_add(1);
						auto latency = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() -
														     transactions[i]->startTime)
								       .count();
						percentile.add(latency);
						if (transactions[i]->distributed_transaction) {
							this->dist_latency.add(latency);
						} else {
							this->local_latency.add(latency);
						}
						record_txn_breakdown_stats(*transactions[i]);
					} else {
						n_abort_lock.fetch_add(1);
						protocol.abort(*transactions[i], messages);
					}
				} else {
					if (transactions[i]->raw) {
						n_abort_lock.fetch_add(1);
						protocol.abort(*transactions[i], messages);
					} else {
						effective_concur++;
						protocol.commit(*transactions[i], messages);
						n_commit.fetch_add(1);
						auto latency = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() -
														     transactions[i]->startTime)
								       .count();
						percentile.add(latency);
						if (transactions[i]->distributed_transaction) {
							this->dist_latency.add(latency);
						} else {
							this->local_latency.add(latency);
						}
						record_txn_breakdown_stats(*transactions[i]);
					}
				}
			}

			if (count % context.batch_flush == 0) {
				flush_messages();
			}
		}
		flush_messages();
		this->round_concurrency.add(concur);
		this->effective_round_concurrency.add(effective_concur);
	}

	void setupHandlers(TransactionType &txn)
	{
		txn.readRequestHandler = [this, &txn](AriaRWKey &readKey, std::size_t tid, uint32_t key_offset) {
			auto table_id = readKey.get_table_id();
			auto partition_id = readKey.get_partition_id();
			const void *key = readKey.get_key();
			void *value = readKey.get_value();
			bool local_index_read = readKey.get_local_index_read_bit();

			bool local_read = false;

			if (this->partitioner->has_master_partition(partition_id)) {
				local_read = true;
			}

			ITable *table = db.find_table(table_id, partition_id);
			if (local_read || local_index_read) {
				// set tid meta_data
				auto row = table->search(key);
				AriaHelper::set_key_tid(readKey, row);
				AriaHelper::read(row, value, table->value_size());
			} else {
				auto coordinatorID = this->partitioner->master_coordinator(partition_id);
				txn.network_size +=
					MessageFactoryType::new_search_message(*(this->messages[coordinatorID]), *table, tid, txn.tid_offset, key, key_offset);
				txn.distributed_transaction = true;
				txn.pendingResponses++;
			}
		};

		txn.remote_request_handler = [this](std::size_t) { return this->process_request(); };
		txn.message_flusher = [this]() { this->flush_messages(); };
	}

	void onExit() override
	{
		std::string transaction_len_str;
		for (size_t i = 0; i < context.straggler_num_txn_len; ++i) {
			transaction_len_str +=
				"wait time " + std::to_string(transaction_lengths[i]) + "us, count " + std::to_string(transaction_lengths_count[i]) + "\n";
		}
		LOG(INFO) << "Transaction Length Info:\n" << transaction_len_str;
		LOG(INFO) << "Worker " << id << " latency: " << percentile.nth(50) << " us (50%) " << percentile.nth(75) << " us (75%) " << percentile.nth(95)
			  << " us (95%) " << percentile.nth(99) << " us (99%). dist txn latency: " << this->dist_latency.nth(50) << " us (50%) "
			  << this->dist_latency.nth(75) << " us (75%) " << this->dist_latency.nth(95) << " us (95%) " << this->dist_latency.nth(99)
			  << " us (99%). local txn latency: " << this->local_latency.nth(50) << " us (50%) " << this->local_latency.nth(75) << " us (75%) "
			  << this->local_latency.nth(95) << " us (95%) " << this->local_latency.nth(99) << " us (99%)"
			  << ". batch concurrency: " << this->round_concurrency.nth(50)
			  << ". effective batch concurrency: " << this->effective_round_concurrency.nth(50)
			  << ". sequencing_time: " << this->sequencing_time.avg() << ". execution time: " << this->execution_time.avg()
			  << ". commit time: " << this->commit_time.avg() << "\n"
			  << " LOCAL count " << this->local_txn_stall_time_pct.size() << " txn stall " << this->local_txn_stall_time_pct.nth(50) << " us, "
			  << " local_work " << this->local_txn_local_work_time_pct.avg() << " us, "
			  << " remote_work " << this->local_txn_remote_work_time_pct.avg() << " us, "
			  << " commit_work " << this->local_txn_commit_work_time_pct.avg() << " us, "
			  << " commit_prepare " << this->local_txn_commit_prepare_time_pct.avg() << " us, "
			  << " commit_persistence " << this->local_txn_commit_persistence_time_pct.avg() << " us, "
			  << " commit_replication " << this->local_txn_commit_replication_time_pct.avg() << " us, "
			  << " commit_write_back " << this->local_txn_commit_write_back_time_pct.avg() << " us, "
			  << " commit_release_lock " << this->local_txn_commit_unlock_time_pct.avg() << " us \n"
			  << " DIST count " << this->dist_txn_stall_time_pct.size() << " txn stall " << this->dist_txn_stall_time_pct.avg() << " us, "
			  << " local_work " << this->dist_txn_local_work_time_pct.avg() << " us, "
			  << " remote_work " << this->dist_txn_remote_work_time_pct.avg() << " us, "
			  << " commit_work " << this->dist_txn_commit_work_time_pct.avg() << " us, "
			  << " commit_prepare " << this->dist_txn_commit_prepare_time_pct.avg() << " us, "
			  << " commit_persistence " << this->dist_txn_commit_persistence_time_pct.avg() << " us, "
			  << " commit_replication " << this->local_txn_commit_replication_time_pct.avg() << " us, "
			  << " commit_write_back " << this->dist_txn_commit_write_back_time_pct.avg() << " us, "
			  << " commit_release_lock " << this->dist_txn_commit_unlock_time_pct.avg() << " us \n";
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
				messageHandlers[type](messagePiece, *messages[message->get_source_node_id()], *table, transactions);
			}

			size += message->get_message_count();
			flush_messages();
		}
		return size;
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

    private:
	DatabaseType &db;
	ContextType context;
	std::vector<std::unique_ptr<TransactionType> > &transactions;
	std::vector<StorageType> storages;
	std::atomic<uint32_t> &epoch, &worker_status, &total_abort;
	std::atomic<uint32_t> &n_complete_workers, &n_started_workers;
	std::unique_ptr<Partitioner> partitioner;
	WorkloadType workload;
	RandomType random;
	ProtocolType protocol;
	WALLogger *logger = nullptr;
	std::unique_ptr<Delay> delay;
	Percentile<int64_t> percentile;
	Percentile<int64_t> local_latency;
	Percentile<int64_t> dist_latency;
	std::vector<std::unique_ptr<Message> > messages;
	std::vector<std::function<void(MessagePiece, Message &, ITable &, std::vector<std::unique_ptr<TransactionType> > &)> > messageHandlers;
	LockfreeQueue<Message *> in_queue, out_queue;
	Percentile<uint64_t> round_concurrency;
	Percentile<uint64_t> effective_round_concurrency;
	Percentile<uint64_t> execution_time;
	Percentile<uint64_t> commit_time;
	Percentile<uint64_t> sequencing_time;
	Percentile<uint64_t> local_txn_stall_time_pct, local_txn_commit_work_time_pct, local_txn_commit_persistence_time_pct, local_txn_commit_prepare_time_pct,
		local_txn_commit_replication_time_pct, local_txn_commit_write_back_time_pct, local_txn_commit_unlock_time_pct, local_txn_local_work_time_pct,
		local_txn_remote_work_time_pct;
	Percentile<uint64_t> dist_txn_stall_time_pct, dist_txn_commit_work_time_pct, dist_txn_commit_persistence_time_pct, dist_txn_commit_prepare_time_pct,
		dist_txn_commit_write_back_time_pct, dist_txn_commit_unlock_time_pct, dist_txn_local_work_time_pct, dist_txn_commit_replication_time_pct,
		dist_txn_remote_work_time_pct;
};
} // namespace aria