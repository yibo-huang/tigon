//
// Created by Yi Lu on 9/11/18.
//

#pragma once

#include "common/CCSet.h"
#include "common/CCHashTable.h"
#include "core/Executor.h"
#include "protocol/TwoPLPasha/TwoPLPasha.h"
#include "protocol/TwoPLPasha/TwoPLPashaHelper.h"
#include "protocol/Pasha/MigrationManager.h"
#include "protocol/Pasha/MigrationManagerFactory.h"
#include "protocol/Pasha/SCCManager.h"
#include "protocol/Pasha/SCCManagerFacrtory.h"

namespace star
{
template <class Workload>
class TwoPLPashaExecutor : public Executor<Workload, TwoPLPasha<typename Workload::DatabaseType> >

{
    public:
	using base_type = Executor<Workload, TwoPLPasha<typename Workload::DatabaseType> >;

	using WorkloadType = Workload;
	using ProtocolType = TwoPLPasha<typename Workload::DatabaseType>;
	using DatabaseType = typename WorkloadType::DatabaseType;
	using TransactionType = typename WorkloadType::TransactionType;
	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;
	using MessageType = typename ProtocolType::MessageType;
	using MessageFactoryType = typename ProtocolType::MessageFactoryType;
	using MessageHandlerType = typename ProtocolType::MessageHandlerType;

	using StorageType = typename WorkloadType::StorageType;

	TwoPLPashaExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db, const ContextType &context, std::atomic<uint32_t> &worker_status,
		      std::atomic<uint32_t> &n_complete_workers, std::atomic<uint32_t> &n_started_workers)
		: base_type(coordinator_id, id, db, context, worker_status, n_complete_workers, n_started_workers)
	{
                if (id == 0) {
                        // create or retrieve the CXL tables
                        std::vector<std::vector<CXLTableBase *> > &cxl_tbl_vecs = db.create_or_retrieve_cxl_tables(context);

                        // init helper
                        twopl_pasha_global_helper = new TwoPLPashaHelper(coordinator_id, cxl_tbl_vecs);
                        CHECK(twopl_pasha_global_helper != nullptr);

                        // init migration manager
                        migration_manager = MigrationManagerFactory::create_migration_manager(context.protocol, context.migration_policy,
                                context.when_to_move_out, context.max_migrated_rows_size);

                        // init software cache-coherence manager
                        scc_manager = SCCManagerFactory::create_scc_manager(context.scc_mechanism);

                        // handle pre-migration
                        if (context.pre_migrate == "None") {
                                // do nothing
                        } else if (context.pre_migrate == "All") {
                                db.move_all_tables_into_cxl(std::bind(&TwoPLPashaHelper::move_from_partition_to_shared_region, twopl_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
                        } else if (context.pre_migrate == "NonPart") {
                                db.move_non_part_tables_into_cxl(std::bind(&TwoPLPashaHelper::move_from_partition_to_shared_region, twopl_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
                        } else {
                                CHECK(0);
                        }

                        // commit metadata init
                        twopl_pasha_global_helper->commit_pasha_metadata_init();
                } else {
                        twopl_pasha_global_helper->wait_for_pasha_metadata_init();
                }
	}

	~TwoPLPashaExecutor() = default;

	void setupHandlers(TransactionType &txn) override
	{
		txn.lock_request_handler = [this, &txn](std::size_t table_id, std::size_t partition_id, uint32_t key_offset, const void *key, void *value,
							bool local_index_read, bool write_lock, bool &success, bool &remote) -> uint64_t {
                        ITable *table = this->db.find_table(table_id, partition_id);

			if (local_index_read) {
				success = true;
				remote = false;
                                auto value_bytes = table->value_size();
                                auto row = table->search(key);
				return twopl_pasha_global_helper->read(row, value, value_bytes, this->n_local_cxl_access);
			}

			if (this->partitioner->has_master_partition(partition_id)) {
                                // statistics
                                this->n_local_access.fetch_add(1);

				remote = false;

				std::atomic<uint64_t> *meta = table->search_metadata(key);
                                if (meta == nullptr) {
                                        return 0;
                                }

				if (write_lock) {
					TwoPLPashaHelper::write_lock(*meta, success);
				} else {
					TwoPLPashaHelper::read_lock(*meta, success);
				}

				if (success) {
                                        auto value_bytes = table->value_size();
                                        auto row = table->search(key);
                                        return twopl_pasha_global_helper->read(row, value, value_bytes, this->n_local_cxl_access);
				} else {
					return 0;
				}

			} else {
                                // statistics
                                this->n_remote_access.fetch_add(1);

                                // multi-host transaction
                                txn.distributed_transaction = true;

                                if (migration_manager->when_to_move_out == MigrationManager::Reactive) {
                                        // update remote hosts involved
                                        auto coordinatorID = this->partitioner->master_coordinator(partition_id);
                                        txn.remote_hosts_involved.insert(coordinatorID);
                                }

                                // I am not the owner of the data
                                char *migrated_row = twopl_pasha_global_helper->get_migrated_row(table_id, partition_id, key, true);
                                if (migrated_row != nullptr) {
                                        remote = false;

                                        // mark it as reference counted so that we know if we need to release it upon commit/abort
                                        txn.readSet[key_offset].set_reference_counted();

                                        if (write_lock) {
                                                TwoPLPashaHelper::remote_write_lock(migrated_row, success);
                                        } else {
                                                TwoPLPashaHelper::remote_read_lock(migrated_row, success);
                                        }

                                        if (success) {
                                                return twopl_pasha_global_helper->remote_read(migrated_row, value, table->value_size());
                                        } else {
                                                return 0;
                                        }
                                } else {
                                        // statistics
                                        this->n_remote_access_with_req.fetch_add(1);

                                        remote = true;

                                        // data is not in the shared region
                                        // ask the remote host to do the data migration
                                        auto coordinatorID = this->partitioner->master_coordinator(partition_id);
                                        txn.network_size += MessageFactoryType::new_data_migration_message(*(this->messages[coordinatorID]), *table, key, txn.transaction_id, key_offset);
                                        txn.pendingResponses++;

                                        return 0;
                                }
			}
		};

                txn.scanRequestHandler = [this, &txn](std::size_t table_id, std::size_t partition_id, uint32_t key_offset, const void *min_key, const void *max_key,
                                                uint64_t limit, int type, void *results) -> bool {
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
                                bool scan_success = true;       // it is possible that the range is empty
                                auto local_scan_processor = [&](const void *key, std::atomic<uint64_t> *meta_ptr, void *data_ptr, bool is_last_tuple) -> bool {
                                        CHECK(key != nullptr);
                                        CHECK(meta_ptr != nullptr);
                                        CHECK(data_ptr != nullptr);

                                        if (limit != 0 && scan_results.size() == limit) {
                                                scan_success = true;
                                                return true;
                                        }

                                        if (table->compare_key(key, max_key) > 0) {
                                                scan_success = true;
                                                return true;
                                        }

                                        CHECK(table->compare_key(key, min_key) >= 0);

                                        // TODO: Theoretically, we need to check if the current tuple is already locked by previous queries.
                                        // But we can ignore it for now because we never generate
                                        // transactions with duplicated or overlapped queries.

                                        // try to acquire the lock
                                        std::atomic<uint64_t> &meta = *reinterpret_cast<std::atomic<uint64_t> *>(meta_ptr);
                                        bool lock_success = false;
                                        if (type == TwoPLPashaRWKey::SCAN_FOR_READ) {
                                                TwoPLPashaHelper::read_lock(meta, lock_success);
                                        } else if (type == TwoPLPashaRWKey::SCAN_FOR_UPDATE) {
                                                TwoPLPashaHelper::write_lock(meta, lock_success);
                                        } else if (type == TwoPLPashaRWKey::SCAN_FOR_DELETE) {
                                                TwoPLPashaHelper::write_lock(meta, lock_success);
                                        }

                                        if (lock_success == true) {
                                                // acquiring lock succeeds, push back the result
                                                ITable::row_entity cur_row(key, table->key_size(), meta_ptr, data_ptr, table->value_size());
                                                scan_results.push_back(cur_row);

                                                // continue scan
                                                return false;
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

                txn.insertRequestHandler = [this, &txn](std::size_t table_id, std::size_t partition_id, uint32_t key_offset, const void *key, void *value) -> bool {
			ITable *table = this->db.find_table(table_id, partition_id);
                        auto value_size = table->value_size();
                        bool local_insert = false;
                        bool ret = true;

			if (this->partitioner->has_master_partition(partition_id) ||
			    (this->partitioner->is_partition_replicated_on(partition_id, this->coordinator_id) && this->context.read_on_replica)) {
				local_insert = true;
			}

			if (local_insert) {
				bool success = table->insert(key, value, true);
                                if (success == false) {
                                        txn.abort_insert = true;
                                        ret = false;
                                }
			} else {
                                CHECK(0);      // right now we only support local insert
			}

                        return ret;
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
