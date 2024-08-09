//
// Created by Yi Lu on 9/11/18.
//

#pragma once

#include "core/group_commit/Executor.h"
#include "protocol/SiloGC/SiloGC.h"

namespace star
{
template <class Workload>
class SiloGCExecutor : public group_commit::Executor<Workload, SiloGC<typename Workload::DatabaseType> >

{
    public:
	using base_type = group_commit::Executor<Workload, SiloGC<typename Workload::DatabaseType> >;

	using WorkloadType = Workload;
	using ProtocolType = SiloGC<typename Workload::DatabaseType>;
	using DatabaseType = typename WorkloadType::DatabaseType;
	using TransactionType = typename WorkloadType::TransactionType;
	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;
	using MessageType = typename ProtocolType::MessageType;
	using MessageFactoryType = typename ProtocolType::MessageFactoryType;
	using MessageHandlerType = typename ProtocolType::MessageHandlerType;

	using StorageType = typename WorkloadType::StorageType;

	SiloGCExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db, const ContextType &context, std::atomic<uint32_t> &worker_status,
		       std::atomic<uint32_t> &n_complete_workers, std::atomic<uint32_t> &n_started_workers)
		: base_type(coordinator_id, id, db, context, worker_status, n_complete_workers, n_started_workers)
	{
	}

	~

		SiloGCExecutor() = default;

	void setupHandlers(TransactionType &txn)

		override
	{
		txn.readRequestHandler = [this, &txn](std::size_t table_id, std::size_t partition_id, uint32_t key_offset, const void *key, void *value,
						      bool local_index_read) -> uint64_t {
			bool local_read = false;

			if (this->partitioner->has_master_partition(partition_id) ||
			    (this->partitioner->is_partition_replicated_on(partition_id, this->coordinator_id) && this->context.read_on_replica)) {
				local_read = true;
			}

			if (local_index_read || local_read) {
				return this->protocol.search(table_id, partition_id, key, value);
			} else {
				ITable *table = this->db.find_table(table_id, partition_id);
				auto coordinatorID = this->partitioner->master_coordinator(partition_id);
				txn.network_size += MessageFactoryType::new_search_message(*(this->sync_messages[coordinatorID]), *table, key, key_offset);
				txn.distributed_transaction = true;
				txn.pendingResponses++;
				return 0;
			}
		};

		txn.remote_request_handler = [this](std::size_t) { return this->process_request(); };
		txn.message_flusher = [this]() { this->flush_sync_messages(); };
		txn.get_table = [this](std::size_t tableId, std::size_t partitionId) { return this->db.find_table(tableId, partitionId); };
	};
};
} // namespace star