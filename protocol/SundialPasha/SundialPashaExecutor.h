//
// Created by Xinjing Zhou Lu on 04/26/22.
//

#pragma once

#include "core/Executor.h"
#include "protocol/SundialPasha/SundialPasha.h"

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
	}

	~SundialPashaExecutor() = default;

	void setupHandlers(TransactionType &txn)

		override
	{
		txn.readRequestHandler = [this, &txn](std::size_t table_id, std::size_t partition_id, uint32_t key_offset, const void *key, void *value,
						      bool local_index_read, bool write_lock) {
			bool local_read = false;

			if (this->partitioner->has_master_partition(partition_id) ||
			    (this->partitioner->is_partition_replicated_on(partition_id, this->coordinator_id) && this->context.read_on_replica)) {
				local_read = true;
			}

			if (local_index_read || local_read) {
				ITable *table = this->db.find_table(table_id, partition_id);
				auto value_size = table->value_size();
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
				return;
			} else {
				ITable *table = this->db.find_table(table_id, partition_id);
				auto coordinatorID = this->partitioner->master_coordinator(partition_id);
				txn.network_size += MessageFactoryType::new_read_message(*(this->messages[coordinatorID]), *table, key, txn.transaction_id,
											 write_lock, key_offset);
				txn.pendingResponses++;
				txn.distributed_transaction = true;
				return;
			}
		};

		txn.remote_request_handler = [this](std::size_t) { return this->process_request(); };
		txn.message_flusher = [this]() { this->flush_messages(); };
		txn.get_table = [this](std::size_t tableId, std::size_t partitionId) { return this->db.find_table(tableId, partitionId); };
		txn.set_logger(this->logger);
	};
};
} // namespace star
