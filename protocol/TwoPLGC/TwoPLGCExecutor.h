//
// Created by Yi Lu on 9/12/18.
//

#pragma once

#include "core/group_commit/Executor.h"
#include "protocol/TwoPLGC/TwoPLGC.h"

namespace star
{
template <class Workload>
class TwoPLGCExecutor : public group_commit::Executor<Workload, TwoPLGC<typename Workload::DatabaseType> >

{
    public:
	using base_type = group_commit::Executor<Workload, TwoPLGC<typename Workload::DatabaseType> >;

	using WorkloadType = Workload;
	using ProtocolType = TwoPLGC<typename Workload::DatabaseType>;
	using DatabaseType = typename WorkloadType::DatabaseType;
	using TransactionType = typename WorkloadType::TransactionType;
	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;
	using MessageType = typename ProtocolType::MessageType;
	using MessageFactoryType = typename ProtocolType::MessageFactoryType;
	using MessageHandlerType = typename ProtocolType::MessageHandlerType;

	using StorageType = typename WorkloadType::StorageType;

	TwoPLGCExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db, const ContextType &context, std::atomic<uint32_t> &worker_status,
			std::atomic<uint32_t> &n_complete_workers, std::atomic<uint32_t> &n_started_workers)
		: base_type(coordinator_id, id, db, context, worker_status, n_complete_workers, n_started_workers)
	{
	}

	~TwoPLGCExecutor() = default;

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

				std::atomic<uint64_t> &tid = *table->search_metadata(key);

				if (write_lock) {
					TwoPLHelper::write_lock(tid, success);
				} else {
					TwoPLHelper::read_lock(tid, success);
				}

				if (success) {
					return this->protocol.search(table_id, partition_id, key, value);
				} else {
					return 0;
				}

			} else {
				remote = true;

				auto coordinatorID = this->partitioner->master_coordinator(partition_id);

				if (write_lock) {
					txn.network_size +=
						MessageFactoryType::new_write_lock_message(*(this->sync_messages[coordinatorID]), *table, key, key_offset);
				} else {
					txn.network_size +=
						MessageFactoryType::new_read_lock_message(*(this->sync_messages[coordinatorID]), *table, key, key_offset);
				}
				txn.distributed_transaction = true;
				return 0;
			}
		};

		txn.remote_request_handler = [this](std::size_t) { return this->process_request(); };
		txn.message_flusher = [this]() { this->flush_sync_messages(); };
		txn.get_table = [this](std::size_t tableId, std::size_t partitionId) { return this->db.find_table(tableId, partitionId); };
	};
};
} // namespace star
