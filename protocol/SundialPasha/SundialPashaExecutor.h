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
                        // init helper
                        new(&global_helper) SundialPashaHelper(coordinator_id, context.coordinator_num, 
                                db.get_table_num_per_partition(), context.partition_num / context.coordinator_num);
                        global_helper.init_pasha_metadata();

                        // init migration manager
                        migration_manager = MigrationManagerFactory::create_migration_manager(context.migration_policy);
                } else {
                        global_helper.wait_for_pasha_metadata_init();
                }
	}

	~SundialPashaExecutor() = default;

	void setupHandlers(TransactionType &txn)

		override
	{
		txn.readRequestHandler = [this, &txn](std::size_t table_id, std::size_t partition_id, uint32_t key_offset, const void *key, void *value,
						      bool local_index_read, bool write_lock) {
			ITable *table = this->db.find_table(table_id, partition_id);
                        auto value_size = table->value_size();
                        bool local_read = false;

			if (this->partitioner->has_master_partition(partition_id) ||
			    (this->partitioner->is_partition_replicated_on(partition_id, this->coordinator_id) && this->context.read_on_replica)) {
				local_read = true;
			}

			if (local_index_read || local_read) {
                                // I am the owner of the data
				auto row = table->search(key);
				bool success = true;

				std::pair<uint64_t, uint64_t> rwts;
				if (write_lock) {
					DCHECK(local_index_read == false);
					success = SundialPashaHelper::write_lock(row, rwts, txn.transaction_id);
				}
				auto read_rwts = SundialPashaHelper::read(row, value, value_size);
				txn.readSet[key_offset].set_wts(read_rwts.first);
				txn.readSet[key_offset].set_rts(read_rwts.second);
				if (write_lock) {
					DCHECK(local_index_read == false);
					if (success) {
						DCHECK(rwts == read_rwts);
						txn.readSet[key_offset].set_write_lock_bit();
					} else {
						txn.abort_lock = true;
					}
				}
			} else {
                                // I am not the owner of the data
                                char *migrated_row = global_helper.get_migrated_row(table_id, partition_id, table->get_plain_key(key), true);
                                if (migrated_row != nullptr) {
                                        // data is in the shared region
                                        bool success = true;

                                        std::pair<uint64_t, uint64_t> rwts;
                                        if (write_lock) {
                                                DCHECK(local_index_read == false);
                                                success = SundialPashaHelper::remote_write_lock(migrated_row, rwts, txn.transaction_id);
                                        }
                                        auto read_rwts = SundialPashaHelper::remote_read(migrated_row, value, value_size);
                                        txn.readSet[key_offset].set_wts(read_rwts.first);
                                        txn.readSet[key_offset].set_rts(read_rwts.second);
                                        if (write_lock) {
                                                DCHECK(local_index_read == false);
                                                if (success) {
                                                        DCHECK(rwts == read_rwts);
                                                        txn.readSet[key_offset].set_write_lock_bit();
                                                } else {
                                                        txn.abort_lock = true;
                                                }
                                        }
                                        // mark it as reference counted so that we know if we need to release it upon commit/abort
                                        txn.readSet[key_offset].set_reference_counted();
                                } else {
                                        // data is not in the shared region
                                        // ask the remote host to do the data migration
                                        auto coordinatorID = this->partitioner->master_coordinator(partition_id);
                                        txn.network_size += MessageFactoryType::new_data_migration_message(*(this->messages[coordinatorID]), *table, key, txn.transaction_id,
                                                                                                write_lock, key_offset);
                                        txn.pendingResponses++;
                                }
                                // multi-host transaction
                                txn.distributed_transaction = true;
			}
                        return;
		};

		txn.remote_request_handler = [this](std::size_t) { return this->process_request(); };
		txn.message_flusher = [this]() { this->flush_messages(); };
		txn.get_table = [this](std::size_t tableId, std::size_t partitionId) { return this->db.find_table(tableId, partitionId); };
		txn.set_logger(this->logger);
	};
};
} // namespace star
