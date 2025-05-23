//
// Created by Yi Lu on 9/11/18.
//

#pragma once

#include "core/Executor.h"
#include "protocol/TwoPL/TwoPL.h"

namespace star
{
template <class Workload>
class TwoPLExecutor : public Executor<Workload, TwoPL<typename Workload::DatabaseType> >

{
    public:
	using base_type = Executor<Workload, TwoPL<typename Workload::DatabaseType> >;

	using WorkloadType = Workload;
	using ProtocolType = TwoPL<typename Workload::DatabaseType>;
	using DatabaseType = typename WorkloadType::DatabaseType;
	using TransactionType = typename WorkloadType::TransactionType;
	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;
	using MessageType = typename ProtocolType::MessageType;
	using MessageFactoryType = typename ProtocolType::MessageFactoryType;
	using MessageHandlerType = typename ProtocolType::MessageHandlerType;

	using StorageType = typename WorkloadType::StorageType;

	TwoPLExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db, const ContextType &context, std::atomic<uint32_t> &worker_status,
		      std::atomic<uint32_t> &n_complete_workers, std::atomic<uint32_t> &n_started_workers)
		: base_type(coordinator_id, id, db, context, worker_status, n_complete_workers, n_started_workers)
	{
	}

	~TwoPLExecutor() = default;

	void setupHandlers(TransactionType &txn) override
	{
		txn.lock_request_handler = [this, &txn](std::size_t table_id, std::size_t partition_id, uint32_t key_offset, const void *key, void *value,
							bool local_index_read, bool write_lock, std::tuple<star::ITable::MetaDataType *, void *> &cached_row,
                                                        bool &success, bool &remote) -> uint64_t {
			if (local_index_read) {
				success = true;
				remote = false;
				return this->protocol.search(table_id, partition_id, key, value);
			}

			ITable *table = this->db.find_table(table_id, partition_id);

			if (this->partitioner->has_master_partition(partition_id)) {
				remote = false;

				auto row = table->search(key);
                                CHECK(std::get<0>(row) != nullptr && std::get<1>(row) != nullptr);

                                cached_row = row;

                                uint64_t tid = 0;

				if (write_lock) {
					tid = TwoPLHelper::take_write_lock_and_read(row, value, table->value_size(), success);
				} else {
					tid = TwoPLHelper::take_read_lock_and_read(row, value, table->value_size(), success);
				}

				if (success == true) {
					return tid;
				} else {
					return 0;
				}
			} else {
				remote = true;

				auto coordinatorID = this->partitioner->master_coordinator(partition_id);

                                txn.pendingResponses++;
				if (write_lock) {
					txn.network_size +=
						MessageFactoryType::new_write_lock_message(*(this->messages[coordinatorID]), *table, key, key_offset);
				} else {
					txn.network_size +=
						MessageFactoryType::new_read_lock_message(*(this->messages[coordinatorID]), *table, key, key_offset);
				}
				txn.distributed_transaction = true;
				return 0;
			}
		};

                txn.scanRequestHandler = [this, &txn](std::size_t table_id, std::size_t partition_id, uint32_t key_offset, const void *min_key, const void *max_key,
                                                uint64_t limit, int type, void *results, ITable::row_entity &next_row_entity) -> bool {
			ITable *table = this->db.find_table(table_id, partition_id);
                        std::vector<ITable::row_entity> &scan_results = *reinterpret_cast<std::vector<ITable::row_entity> *>(results);
                        auto value_size = table->value_size();
                        bool local_scan = false;

			if (this->partitioner->has_master_partition(partition_id) ||
			    (this->partitioner->is_partition_replicated_on(partition_id, this->coordinator_id) && this->context.read_on_replica)) {
				local_scan = true;
			}

                        if (local_scan) {
                                // we do the next-key locking logic inside this function
                                bool scan_success = true;       // it is possible that the range is empty - we accept this case
                                auto local_scan_processor = [&](const void *key, std::atomic<uint64_t> *meta_ptr, void *data_ptr, bool is_last_tuple) -> bool {
                                        CHECK(key != nullptr);
                                        CHECK(meta_ptr != nullptr);
                                        CHECK(data_ptr != nullptr);

                                        bool locking_next_tuple = false;

                                        if (is_last_tuple == true) {
                                                locking_next_tuple = true;
                                        } else if (limit != 0 && scan_results.size() == limit) {
                                                locking_next_tuple = true;
                                        } else if (table->compare_key(key, max_key) > 0) {
                                                locking_next_tuple = true;
                                        }

                                        if (table->compare_key(key, min_key) >= 0) {
                                                if (scan_results.size() > 0) {
                                                        if (table->compare_key(key, scan_results[scan_results.size() - 1].key) <= 0) {
                                                                return false;
                                                        }
                                                }
                                        } else {
                                                return false;
                                        }

                                        // TODO: Theoretically, we need to check if the current tuple is already locked by previous queries.
                                        // But we can ignore it for now because we never generate
                                        // transactions with duplicated or overlapped queries.

                                        // try to acquire the lock
                                        std::atomic<uint64_t> &meta = *reinterpret_cast<std::atomic<uint64_t> *>(meta_ptr);
                                        bool lock_success = false;
                                        if (type == TwoPLRWKey::SCAN_FOR_READ) {
                                                TwoPLHelper::read_lock(meta, lock_success);
                                        } else if (type == TwoPLRWKey::SCAN_FOR_UPDATE) {
                                                TwoPLHelper::write_lock(meta, lock_success);
                                        } else if (type == TwoPLRWKey::SCAN_FOR_INSERT) {
                                                TwoPLHelper::write_lock(meta, lock_success);
                                        } else if (type == TwoPLRWKey::SCAN_FOR_DELETE) {
                                                TwoPLHelper::write_lock(meta, lock_success);
                                        } else {
                                                CHECK(0);
                                        }

                                        if (lock_success == true) {
                                                // acquiring lock succeeds
                                                ITable::row_entity cur_row(key, table->key_size(), meta_ptr, data_ptr, table->value_size());
                                                if (locking_next_tuple == false) {
                                                        scan_results.push_back(cur_row);
                                                        // continue scan
                                                        return false;
                                                } else {
                                                        // scan succeeds - store the next-tuple and quit
                                                        next_row_entity = cur_row;
                                                        scan_success = true;
                                                        return true;
                                                }
                                        } else {
                                                // stop and fail immediately if we fail to acquire a lock
                                                scan_success = false;
                                                return true;
                                        }
                                };

                                table->scan(min_key, local_scan_processor);
                                return scan_success;
                        } else {
                                CHECK(0);      // right now we only support local scan
                        }
		};

                txn.insertRequestHandler = [this, &txn](std::size_t table_id, std::size_t partition_id, uint32_t key_offset,
                                                        const void *key, void *value, bool require_lock_next_key, ITable::row_entity &next_row_entity) -> bool {
			ITable *table = this->db.find_table(table_id, partition_id);
                        auto value_size = table->value_size();
                        bool local_insert = false;

			if (this->partitioner->has_master_partition(partition_id) ||
			    (this->partitioner->is_partition_replicated_on(partition_id, this->coordinator_id) && this->context.read_on_replica)) {
				local_insert = true;
			}

			if (local_insert) {
                                auto local_next_key_processor = [&](const void *next_key, std::atomic<uint64_t> *meta_ptr, void *data_ptr) -> bool {
                                        CHECK(key != nullptr);
                                        CHECK(meta_ptr != nullptr);
                                        CHECK(data_ptr != nullptr);

                                        // try to acquire the read lock of the next key
                                        std::atomic<uint64_t> &meta = *reinterpret_cast<std::atomic<uint64_t> *>(meta_ptr);
                                        bool lock_success = false;
                                        TwoPLHelper::write_lock(meta, lock_success);
                                        if (lock_success == true) {
                                                ITable::row_entity next_row(next_key, table->key_size(), meta_ptr, data_ptr, table->value_size());
                                                next_row_entity = next_row;
                                        }

                                        return lock_success;
                                };

                                bool insert_success = false;
                                if (require_lock_next_key == true) {
				        insert_success = table->insert_lock_next_key(key, value, local_next_key_processor, true);
                                } else {
                                        insert_success = table->insert(key, value, true);
                                }

                                if (insert_success == false) {
                                        txn.abort_insert = true;
                                        return false;
                                } else {
                                        return true;
                                }
			} else {
                                CHECK(0);      // right now we only support local insert
			}
		};


                txn.deleteRequestHandler = [this, &txn](std::size_t table_id, std::size_t partition_id, uint32_t key_offset, const void *key) -> bool {
			ITable *table = this->db.find_table(table_id, partition_id);
                        auto value_size = table->value_size();
                        bool local_delete = false;

			if (this->partitioner->has_master_partition(partition_id) ||
			    (this->partitioner->is_partition_replicated_on(partition_id, this->coordinator_id) && this->context.read_on_replica)) {
				local_delete = true;
			}

			if (local_delete) {
                                // do nothing here
                                // we assume all the deletes are "read and delete"
                                // so the write locks should already been taken
			} else {
                                CHECK(0);      // right now we only support local delete
			}

                        return true;
		};

		txn.remote_request_handler = [this](std::size_t) { return this->process_request(); };
		txn.message_flusher = [this]() { this->flush_messages(); };
		txn.get_table = [this](std::size_t tableId, std::size_t partitionId) { return this->db.find_table(tableId, partitionId); };
		txn.set_logger(this->logger);
	};
};
} // namespace star
