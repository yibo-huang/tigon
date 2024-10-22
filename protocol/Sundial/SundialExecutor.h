//
// Created by Xinjing Zhou Lu on 04/26/22.
//

#pragma once

#include "core/Executor.h"
#include "protocol/Sundial/Sundial.h"

namespace star
{
template <class Workload>
class SundialExecutor : public Executor<Workload, Sundial<typename Workload::DatabaseType> >

{
    public:
	using base_type = Executor<Workload, Sundial<typename Workload::DatabaseType> >;

	using WorkloadType = Workload;
	using ProtocolType = Sundial<typename Workload::DatabaseType>;
	using DatabaseType = typename WorkloadType::DatabaseType;
	using TransactionType = typename WorkloadType::TransactionType;
	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;
	using MessageType = typename ProtocolType::MessageType;
	using MessageFactoryType = typename ProtocolType::MessageFactoryType;
	using MessageHandlerType = typename ProtocolType::MessageHandlerType;

	using StorageType = typename WorkloadType::StorageType;

	SundialExecutor(std::size_t coordinator_id, std::size_t id, DatabaseType &db, const ContextType &context, std::atomic<uint32_t> &worker_status,
			std::atomic<uint32_t> &n_complete_workers, std::atomic<uint32_t> &n_started_workers)
		: base_type(coordinator_id, id, db, context, worker_status, n_complete_workers, n_started_workers)
	{
	}

	~SundialExecutor() = default;

	void setupHandlers(TransactionType &txn)

		override
	{
		txn.readRequestHandler = [this, &txn](std::size_t table_id, std::size_t partition_id, uint32_t key_offset, const void *key, void *value,
						      bool local_index_read, bool write_lock) -> bool {
			bool local_read = false;
                        bool ret = true;

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
					success = SundialHelper::write_lock(row, rwts, txn.transaction_id);
				}
				auto read_rwts = SundialHelper::read(row, value, value_size);
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
				ITable *table = this->db.find_table(table_id, partition_id);
				auto coordinatorID = this->partitioner->master_coordinator(partition_id);
				txn.network_size += MessageFactoryType::new_read_message(*(this->messages[coordinatorID]), *table, key, txn.transaction_id,
											 write_lock, key_offset);
				txn.pendingResponses++;
				txn.distributed_transaction = true;
			}

                        return ret;
		};

                txn.scanRequestHandler = [this, &txn](std::size_t table_id, std::size_t partition_id, uint32_t key_offset, const void *min_key, const void *max_key,
                                                uint64_t limit, void *results) -> bool {
			ITable *table = this->db.find_table(table_id, partition_id);
                        std::vector<ITable::single_scan_result> &scan_results = *reinterpret_cast<std::vector<ITable::single_scan_result> *>(results);
                        auto value_size = table->value_size();
                        bool local_scan = false;

			if (this->partitioner->has_master_partition(partition_id) ||
			    (this->partitioner->is_partition_replicated_on(partition_id, this->coordinator_id) && this->context.read_on_replica)) {
				local_scan = true;
			}

			if (local_scan) {
                                auto local_scan_processor = [&](const void *key, std::atomic<uint64_t> *meta_ptr, void *data_ptr) -> bool {
                                        CHECK(key != nullptr);
                                        CHECK(meta_ptr != nullptr);
                                        CHECK(data_ptr != nullptr);

                                        if (limit != 0 && scan_results.size() == limit) {
                                                return true;
                                        }

                                        if (table->compare_key(key, max_key) > 0) {
                                                return true;
                                        }

                                        CHECK(table->compare_key(key, min_key) >= 0);

                                        ITable::single_scan_result cur_row(key, table->key_size(), meta_ptr, data_ptr, table->value_size());
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
                                bool success = SundialHelper::write_lock(row, rwts, txn.transaction_id);
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
