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
                        twopl_pasha_global_helper = new TwoPLPashaHelper(coordinator_id, context, cxl_tbl_vecs);
                        DCHECK(twopl_pasha_global_helper != nullptr);

                        // init migration manager
                        uint64_t hw_cc_budget_per_host = (context.hw_cc_budget - CXL_EBR::max_ebr_retiring_memory) / context.coordinator_num;
                        LOG(INFO) << "total hardware budget = " << context.hw_cc_budget << " per host = " << hw_cc_budget_per_host;
                        migration_manager = MigrationManagerFactory::create_migration_manager(context.protocol, context.migration_policy, context.coordinator_id,
                                context.partition_num, context.when_to_move_out, hw_cc_budget_per_host);

                        // init software cache-coherence manager
                        scc_manager = SCCManagerFactory::create_scc_manager(context.protocol, context.scc_mechanism);

                        // handle pre-migration
                        if (context.pre_migrate == "None") {
                                // do nothing
                        } else if (context.pre_migrate == "All") {
                                db.move_all_tables_into_cxl(std::bind(&MigrationManager::move_row_in, migration_manager, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4));
                        } else if (context.pre_migrate == "NonPart") {
                                db.move_non_part_tables_into_cxl(std::bind(&MigrationManager::move_row_in, migration_manager, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4));
                        } else {
                                DCHECK(0);
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
							bool local_index_read, bool write_lock, std::tuple<star::ITable::MetaDataType *, void *> &cached_local_row,
                                                        char *&cached_migrated_row, bool &success, bool &remote) -> uint64_t {
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

				auto row = table->search(key);
                                DCHECK(std::get<0>(row) != nullptr && std::get<1>(row) != nullptr);

                                if (this->context.model_cxl_search_overhead == true) {
                                        twopl_pasha_global_helper->model_cxl_search_overhead(row, table_id, partition_id, key);
                                }

                                cached_local_row = row;

                                uint64_t tid = 0;

				if (write_lock) {
					tid = twopl_pasha_global_helper->take_write_lock_and_read(row, value, table->value_size(), success, this->n_local_cxl_access);
				} else {
					tid = twopl_pasha_global_helper->take_read_lock_and_read(row, value, table->value_size(), success, this->n_local_cxl_access);
				}

				if (success == true) {
					return tid;
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

                                        // cache migrated row pointer
                                        cached_migrated_row = migrated_row;

                                        uint64_t tid = 0;

                                        // mark it as reference counted so that we know if we need to release it upon commit/abort
                                        txn.readSet[key_offset].set_reference_counted();

                                        if (write_lock) {
                                                tid = twopl_pasha_global_helper->remote_take_write_lock_and_read(migrated_row, value, table->value_size(), false, success);
                                        } else {
                                                tid = twopl_pasha_global_helper->remote_take_read_lock_and_read(migrated_row, value, table->value_size(), false, success);
                                        }

                                        if (success == true) {
                                                return tid;
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

                if (this->context.enable_phantom_detection == true) {
                        txn.scanRequestHandler = [this, &txn](std::size_t table_id, std::size_t partition_id, uint32_t key_offset, const void *min_key, const void *max_key,
                                                uint64_t limit, int type, void *results, ITable::row_entity &next_row_entity, bool &migration_required) -> bool {
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
                                                DCHECK(key != nullptr);
                                                DCHECK(meta_ptr != nullptr);
                                                DCHECK(data_ptr != nullptr);

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
                                                if (type == TwoPLPashaRWKey::SCAN_FOR_READ) {
                                                        twopl_pasha_global_helper->read_lock(meta, data_ptr, table->value_size(), lock_success);
                                                } else if (type == TwoPLPashaRWKey::SCAN_FOR_UPDATE) {
                                                        twopl_pasha_global_helper->write_lock(meta, data_ptr, table->value_size(), lock_success);
                                                } else if (type == TwoPLPashaRWKey::SCAN_FOR_INSERT) {
                                                        twopl_pasha_global_helper->write_lock(meta, data_ptr, table->value_size(), lock_success);
                                                } else if (type == TwoPLPashaRWKey::SCAN_FOR_DELETE) {
                                                        twopl_pasha_global_helper->write_lock(meta, data_ptr, table->value_size(), lock_success);
                                                } else {
                                                        DCHECK(0);
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
                                        migration_required = false;     // local scan does not require data migration
                                        return scan_success;
                                } else {
                                        // we do the next-key locking logic inside this function
                                        bool scan_success = false;       // it is possible that the range is empty - we return fail and abort in this case
                                        auto remote_scan_processor = [&](const void *key, void *cxl_row, bool is_last_tuple) -> bool {
                                                DCHECK(key != nullptr);
                                                DCHECK(cxl_row != nullptr);

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

                                                // check if the previous key and the next key are real
                                                TwoPLPashaMetadataShared *smeta = reinterpret_cast<TwoPLPashaMetadataShared *>(cxl_row);
                                                TwoPLPashaSharedDataSCC *scc_data = smeta->get_scc_data();

                                                smeta->lock();
                                                if (table->compare_key(key, min_key) == 0) {
                                                        // if the first key matches the min_key, then we do not care about the previous key
                                                        DCHECK(scan_results.size() == 0);
                                                        if (smeta->get_next_key_real_bit() == false) {
                                                                migration_required = true;
                                                        }
                                                } else if (scan_results.size() == limit) {
                                                        // we do not care about the next key of the next key
                                                        if (smeta->get_prev_key_real_bit() == false) {
                                                                migration_required = true;
                                                        }
                                                } else {
                                                        // for intermediate keys, we care about both the next and previous keys
                                                        if (smeta->get_next_key_real_bit() == false || smeta->get_prev_key_real_bit() == false) {
                                                                migration_required = true;
                                                        }
                                                }
                                                smeta->unlock();

                                                // stop immediately if data migration is needed
                                                if (migration_required == true) {
                                                        scan_success = false;
                                                        return true;
                                                }

                                                // TODO: Theoretically, we need to check if the current tuple is already locked by previous queries.
                                                // But we can ignore it for now because we never generate
                                                // transactions with duplicated or overlapped queries.

                                                // try to acquire the lock and increase the reference count if locking succeeds
                                                bool lock_success = false;
                                                if (type == TwoPLPashaRWKey::SCAN_FOR_READ) {
                                                        twopl_pasha_global_helper->remote_read_lock_and_inc_ref_cnt(reinterpret_cast<char *>(cxl_row), table->value_size(), lock_success);
                                                } else if (type == TwoPLPashaRWKey::SCAN_FOR_UPDATE) {
                                                        twopl_pasha_global_helper->remote_write_lock_and_inc_ref_cnt(reinterpret_cast<char *>(cxl_row), table->value_size(), lock_success);
                                                } else if (type == TwoPLPashaRWKey::SCAN_FOR_INSERT) {
                                                        twopl_pasha_global_helper->remote_write_lock_and_inc_ref_cnt(reinterpret_cast<char *>(cxl_row), table->value_size(), lock_success);
                                                } else if (type == TwoPLPashaRWKey::SCAN_FOR_DELETE) {
                                                        twopl_pasha_global_helper->remote_write_lock_and_inc_ref_cnt(reinterpret_cast<char *>(cxl_row), table->value_size(), lock_success);
                                                } else {
                                                        DCHECK(0);
                                                }

                                                if (lock_success == true) {
                                                        // acquiring lock succeeds
                                                        ITable::row_entity cur_row(key, table->key_size(), reinterpret_cast<std::atomic<uint64_t> *>(cxl_row), scc_data->data, table->value_size());
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

                                        CXLTableBase *target_cxl_table = twopl_pasha_global_helper->get_cxl_table(table_id, partition_id);
                                        target_cxl_table->scan(min_key, remote_scan_processor);

                                        // we assume that every scan will return at least one value - similar to our assumption on point queries
                                        // so if the results are none, we need to do data migration!
                                        if (scan_results.size() == 0 && scan_success == false) {
                                                migration_required = true;
                                        }

                                        if (migration_required == true) {
                                                DCHECK(scan_success == false);
                                                // data is not in the shared region
                                                // ask the remote host to do the data migration
                                                auto coordinatorID = this->partitioner->master_coordinator(partition_id);
                                                txn.network_size += MessageFactoryType::new_data_migration_message_for_scan(*(this->messages[coordinatorID]), *table, min_key, max_key, limit, txn.transaction_id, key_offset);
                                                txn.pendingResponses++;

                                                // release locks
                                                for (auto i = 0u; i < scan_results.size(); i++) {
                                                        auto cxl_row = scan_results[i].meta;
                                                        if (type == TwoPLPashaRWKey::SCAN_FOR_READ) {
                                                                TwoPLPashaHelper::remote_read_lock_release(reinterpret_cast<char *>(cxl_row));
                                                        } else if (type == TwoPLPashaRWKey::SCAN_FOR_UPDATE) {
                                                                TwoPLPashaHelper::remote_write_lock_release(reinterpret_cast<char *>(cxl_row));
                                                        } else if (type == TwoPLPashaRWKey::SCAN_FOR_INSERT) {
                                                                TwoPLPashaHelper::remote_write_lock_release(reinterpret_cast<char *>(cxl_row));
                                                        } else if (type == TwoPLPashaRWKey::SCAN_FOR_DELETE) {
                                                                TwoPLPashaHelper::remote_write_lock_release(reinterpret_cast<char *>(cxl_row));
                                                        } else {
                                                                DCHECK(0);
                                                        }
                                                        TwoPLPashaHelper::decrease_reference_count_via_ptr(cxl_row);
                                                }

                                                scan_results.clear();
                                        }
                                        return scan_success;
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
                                        bool insert_success = twopl_pasha_global_helper->insert_and_update_next_key_info(table, key, value, require_lock_next_key, next_row_entity);
                                        if (insert_success == false) {
                                                txn.abort_insert = true;
                                                return false;
                                        } else {
                                                return true;
                                        }
                                } else {
                                        // instead of sending remote insert request here,
                                        // we send it in the commit phase such that we do not need to worry about aborting.
                                        return true;
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
                                        // do nothing here
                                        // we assume all the deletes are "read and delete"
                                        // so the write locks should already been taken
                                }

                                return true;
                        };
                } else {
                        txn.scanRequestHandler = [this, &txn](std::size_t table_id, std::size_t partition_id, uint32_t key_offset, const void *min_key, const void *max_key,
                                                uint64_t limit, int type, void *results, ITable::row_entity &next_row_entity, bool &migration_required) -> bool {
                                ITable *table = this->db.find_table(table_id, partition_id);
                                std::vector<ITable::row_entity> &scan_results = *reinterpret_cast<std::vector<ITable::row_entity> *>(results);
                                auto value_size = table->value_size();
                                bool local_scan = false;

                                if (this->partitioner->has_master_partition(partition_id) ||
                                        (this->partitioner->is_partition_replicated_on(partition_id, this->coordinator_id) && this->context.read_on_replica)) {
                                        local_scan = true;
                                }

                                if (local_scan) {
                                        auto local_scan_processor = [&](const void *key, std::atomic<uint64_t> *meta_ptr, void *data_ptr, bool is_last_tuple) -> bool {
                                                DCHECK(key != nullptr);
                                                DCHECK(meta_ptr != nullptr);
                                                DCHECK(data_ptr != nullptr);

                                                if (limit != 0 && scan_results.size() == limit) {
                                                        return true;
                                                }

                                                if (table->compare_key(key, max_key) > 0) {
                                                        return true;
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

                                                ITable::row_entity cur_row(key, table->key_size(), meta_ptr, data_ptr, table->value_size());
                                                scan_results.push_back(cur_row);

                                                // continue scan
                                                return false;
                                        };

                                        table->scan(min_key, local_scan_processor);
                                } else {
                                        CHECK(0);      // right now we only support local scan
                                }

                                return true;
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
                                        bool insert_success = table->insert(key, value, true);
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
                                        auto row = table->search(key);
                                        if (std::get<0>(row) == nullptr && std::get<1>(row) == nullptr) {
                                                // someone else has deleted the row, so we abort
                                                txn.abort_delete = true;
                                                return false;
                                        }

                                        std::atomic<uint64_t> &meta = *reinterpret_cast<std::atomic<uint64_t> *>(std::get<0>(row));
                                        void *data_ptr = std::get<1>(row);
                                        bool lock_success = false;
                                        twopl_pasha_global_helper->write_lock(meta, data_ptr, table->value_size(), lock_success);

                                        if (lock_success) {
                                                return true;
                                        } else {
                                                txn.abort_delete = true;
                                                return false;
                                        }
                                } else {
                                        CHECK(0);      // right now we only support local delete
                                }
                        };
                };

		txn.remote_request_handler = [this](std::size_t) { return this->process_request(); };
		txn.message_flusher = [this]() { this->flush_messages(); };
		txn.get_table = [this](std::size_t tableId, std::size_t partitionId) { return this->db.find_table(tableId, partitionId); };
		txn.set_logger(this->logger);
	};
};
} // namespace star
