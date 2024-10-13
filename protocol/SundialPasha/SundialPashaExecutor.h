//
// Created by Xinjing Zhou Lu on 04/26/22.
//

#pragma once

#include "common/CCSet.h"
#include "common/CCHashTable.h"
#include "core/Executor.h"
#include "protocol/SundialPasha/SundialPasha.h"
#include "protocol/SundialPasha/SundialPashaHelper.h"
#include "protocol/Pasha/MigrationManager.h"
#include "protocol/Pasha/MigrationManagerFactory.h"
#include "protocol/Pasha/SCCManager.h"
#include "protocol/Pasha/SCCManagerFacrtory.h"

namespace star
{
template <class Workload>
class SundialPashaExecutor : public Executor<Workload, SundialPasha<typename Workload::DatabaseType> >

{
    public:
	using base_type = Executor<Workload, SundialPasha<typename Workload::DatabaseType> >;

	using WorkloadType = Workload;
	using ProtocolType = SundialPasha<typename Workload::DatabaseType>;
	using DatabaseType = typename WorkloadType::DatabaseType;
	using TransactionType = typename WorkloadType::TransactionType;
	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;
	using MessageType = typename ProtocolType::MessageType;
	using MessageFactoryType = typename ProtocolType::MessageFactoryType;
	using MessageHandlerType = typename ProtocolType::MessageHandlerType;

	using StorageType = typename WorkloadType::StorageType;

	SundialPashaExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db, const ContextType &context, std::atomic<uint32_t> &worker_status,
			std::atomic<uint32_t> &n_complete_workers, std::atomic<uint32_t> &n_started_workers)
		: base_type(coordinator_id, id, db, context, worker_status, n_complete_workers, n_started_workers)
	{
                if (id == 0) {
                        // create or retrieve the CXL tables
                        std::vector<std::vector<CXLTableBase *> > &cxl_tbl_vecs = db.create_or_retrieve_cxl_tables(context);

                        // init helper
                        sundial_pasha_global_helper = new SundialPashaHelper(coordinator_id, cxl_tbl_vecs);

                        // init migration manager
                        migration_manager = MigrationManagerFactory::create_migration_manager(context.protocol, context.migration_policy,
                                context.when_to_move_out, context.max_migrated_rows_size);

                        // init software cache-coherence manager
                        scc_manager = SCCManagerFactory::create_scc_manager(context.scc_mechanism);

                        // handle pre-migration
                        if (context.pre_migrate == "None") {
                                // do nothing
                        } else if (context.pre_migrate == "All") {
                                db.move_all_tables_into_cxl(std::bind(&SundialPashaHelper::move_from_partition_to_shared_region, sundial_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
                        } else if (context.pre_migrate == "NonPart") {
                                db.move_non_part_tables_into_cxl(std::bind(&SundialPashaHelper::move_from_partition_to_shared_region, sundial_pasha_global_helper, std::placeholders::_1, std::placeholders::_2, std::placeholders::_3));
                        } else {
                                CHECK(0);
                        }

                        // commit metadata init
                        sundial_pasha_global_helper->commit_pasha_metadata_init();
                } else {
                        sundial_pasha_global_helper->wait_for_pasha_metadata_init();
                }
	}

	~SundialPashaExecutor() = default;

	void setupHandlers(TransactionType &txn)

		override
	{
		txn.readRequestHandler = [this, &txn](std::size_t table_id, std::size_t partition_id, uint32_t key_offset, const void *key, void *value,
						      bool local_index_read, bool write_lock) -> bool  {
			ITable *table = this->db.find_table(table_id, partition_id);
                        auto value_size = table->value_size();
                        bool local_read = false;
                        bool ret = true;

			if (this->partitioner->has_master_partition(partition_id) ||
			    (this->partitioner->is_partition_replicated_on(partition_id, this->coordinator_id) && this->context.read_on_replica)) {
				local_read = true;
			}

			if (local_index_read || local_read) {
                                // statistics
                                this->n_local_access.fetch_add(1);

                                // I am the owner of the data
				auto row = table->search(key);
                                CHECK(std::get<0>(row) != nullptr && std::get<1>(row) != nullptr);
				bool success = true;

				std::pair<uint64_t, uint64_t> rwts;
				if (write_lock) {
					DCHECK(local_index_read == false);
					success = SundialPashaHelper::write_lock(row, rwts, txn.transaction_id);
				}
				auto read_rwts = sundial_pasha_global_helper->read(row, value, value_size, this->n_local_cxl_access);
				txn.readSet[key_offset].set_wts(read_rwts.first);
				txn.readSet[key_offset].set_rts(read_rwts.second);
				if (write_lock) {
					DCHECK(local_index_read == false);
					if (success) {
						DCHECK(rwts == read_rwts);
						txn.readSet[key_offset].set_write_lock_bit();
					} else {
						txn.abort_lock = true;
                                                ret = false;
					}
				}
			} else {
                                // statistics
                                this->n_remote_access.fetch_add(1);

                                // I am not the owner of the data
                                char *migrated_row = sundial_pasha_global_helper->get_migrated_row(table_id, partition_id, key, true);
                                if (migrated_row != nullptr) {
                                        // data is in the shared region
                                        bool success = true;

                                        std::pair<uint64_t, uint64_t> rwts;
                                        if (write_lock) {
                                                DCHECK(local_index_read == false);
                                                success = SundialPashaHelper::remote_write_lock(migrated_row, rwts, txn.transaction_id);
                                        }
                                        auto read_rwts = sundial_pasha_global_helper->remote_read(migrated_row, value, value_size);
                                        txn.readSet[key_offset].set_wts(read_rwts.first);
                                        txn.readSet[key_offset].set_rts(read_rwts.second);
                                        if (write_lock) {
                                                DCHECK(local_index_read == false);
                                                if (success) {
                                                        DCHECK(rwts == read_rwts);
                                                        txn.readSet[key_offset].set_write_lock_bit();
                                                } else {
                                                        txn.abort_lock = true;
                                                        ret = false;
                                                }
                                        }
                                        // mark it as reference counted so that we know if we need to release it upon commit/abort
                                        txn.readSet[key_offset].set_reference_counted();
                                } else {
                                        // statistics
                                        this->n_remote_access_with_req.fetch_add(1);

                                        // data is not in the shared region
                                        // ask the remote host to do the data migration
                                        auto coordinatorID = this->partitioner->master_coordinator(partition_id);
                                        txn.network_size += MessageFactoryType::new_data_migration_message(*(this->messages[coordinatorID]), *table, key, txn.transaction_id, key_offset);
                                        txn.pendingResponses++;
                                }
                                // multi-host transaction
                                txn.distributed_transaction = true;

                                if (migration_manager->when_to_move_out == MigrationManager::Reactive) {
                                        // update remote hosts involved
                                        auto coordinatorID = this->partitioner->master_coordinator(partition_id);
                                        txn.remote_hosts_involved.insert(coordinatorID);
                                }
			}

                        return ret;
		};

                txn.scanRequestHandler = [this, &txn](std::size_t table_id, std::size_t partition_id, uint32_t key_offset, const void *min_key, const void *max_key,
                                                uint64_t limit, void *results) -> bool {
			ITable *table = this->db.find_table(table_id, partition_id);
                        auto value_size = table->value_size();
                        bool local_scan = false;

			if (this->partitioner->has_master_partition(partition_id) ||
			    (this->partitioner->is_partition_replicated_on(partition_id, this->coordinator_id) && this->context.read_on_replica)) {
				local_scan = true;
			}

			if (local_scan) {
				table->scan(min_key, max_key, limit, results);
			} else {
                                CHECK(0);      // right now we only support local scan
			}

                        return true;
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
				auto row = table->search(key);
                                if (std::get<0>(row) == nullptr && std::get<1>(row) == nullptr) {
                                        // someone else has deleted the row, so we abort
                                        txn.abort_delete = true;
                                        return false;
                                }

                                std::pair<uint64_t, uint64_t> rwts;
                                bool success = SundialPashaHelper::write_lock(row, rwts, txn.transaction_id);
                                if (success) {
                                        // do nothing
                                } else {
                                        txn.abort_delete = true;
                                        return false;
                                }
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
