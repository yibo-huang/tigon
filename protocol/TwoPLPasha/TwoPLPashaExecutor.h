//
// Created by Yi Lu on 9/11/18.
//

#pragma once

#include "core/Executor.h"
#include "protocol/TwoPLPasha/TwoPLPasha.h"

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
	}

	~TwoPLPashaExecutor() = default;

	void setupHandlers(TransactionType &txn) override
	{
		txn.lock_request_handler = [this, &txn](std::size_t table_id, std::size_t partition_id, uint32_t key_offset, const void *key, void *value,
							bool local_index_read, bool write_lock, bool &success, bool &remote) -> uint64_t {
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
					TwoPLPashaHelper::write_lock(tid, success);
				} else {
					TwoPLPashaHelper::read_lock(tid, success);
				}

				if (success) {
					return this->protocol.search(table_id, partition_id, key, value);
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
                                std::atomic<uint64_t> *tid = table->search_metadata(key);
                                if (tid == nullptr) {
                                        // someone else has deleted the row, so we abort
                                        txn.abort_delete = true;
                                        return false;
                                }

                                bool success = false;
                                TwoPLPashaHelper::write_lock(*tid, success);
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
