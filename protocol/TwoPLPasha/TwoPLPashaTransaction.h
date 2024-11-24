//
// Created by Yi Lu on 9/11/18.
//

#pragma once

#include "common/Message.h"
#include "common/WALLogger.h"
#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/TwoPLPasha/TwoPLPashaRWKey.h"
#include <chrono>
#include <glog/logging.h>
#include <vector>

namespace star
{
class TwoPLPashaTransaction {
    public:
	using MetaDataType = std::atomic<uint64_t>;

	TwoPLPashaTransaction(std::size_t coordinator_id, std::size_t partition_id, Partitioner &partitioner, std::size_t ith_replica)
		: coordinator_id(coordinator_id)
		, partition_id(partition_id)
		, startTime(std::chrono::steady_clock::now())
		, partitioner(partitioner)
		, ith_replica(ith_replica)
	{
		reset();
	}

	virtual ~TwoPLPashaTransaction() = default;

	void set_logger(WALLogger *logger)
	{
		this->logger = logger;
	}

	WALLogger *get_logger()
	{
		return this->logger;
	}

	std::size_t commit_unlock_time_us = 0;
	std::size_t commit_work_time_us = 0;
	std::size_t commit_write_back_time_us = 0;
	std::size_t remote_work_time_us = 0;
	std::size_t local_work_time_us = 0;
	std::size_t stall_time_us = 0; // Waiting for locks (partition-level or row-level) due to conflicts

	std::size_t commit_prepare_time_us = 0;
	std::size_t commit_persistence_time_us = 0;
	std::size_t commit_replication_time_us = 0;
	virtual void record_commit_replication_time(uint64_t us)
	{
		commit_replication_time_us += us;
	}

	virtual size_t get_commit_replication_time()
	{
		return commit_replication_time_us;
	}
	virtual void record_commit_persistence_time(uint64_t us)
	{
		commit_persistence_time_us += us;
	}

	virtual size_t get_commit_persistence_time()
	{
		return commit_persistence_time_us;
	}

	virtual void record_commit_prepare_time(uint64_t us)
	{
		commit_prepare_time_us += us;
	}

	virtual size_t get_commit_prepare_time()
	{
		return commit_prepare_time_us;
	}

	virtual void record_remote_work_time(uint64_t us)
	{
		remote_work_time_us += us;
	}

	virtual size_t get_remote_work_time()
	{
		return remote_work_time_us;
	}

	virtual void record_local_work_time(uint64_t us)
	{
		local_work_time_us += us;
	}

	virtual size_t get_local_work_time()
	{
		return local_work_time_us;
	}

	virtual void record_commit_work_time(uint64_t us)
	{
		commit_work_time_us += us;
	}

	virtual size_t get_commit_work_time()
	{
		return commit_work_time_us;
	}

	virtual void record_commit_write_back_time(uint64_t us)
	{
		commit_write_back_time_us += us;
	}

	virtual size_t get_commit_write_back_time()
	{
		return commit_write_back_time_us;
	}

	virtual void record_commit_unlock_time(uint64_t us)
	{
		commit_unlock_time_us += us;
	}

	virtual size_t get_commit_unlock_time()
	{
		return commit_unlock_time_us;
	}

	virtual void set_stall_time(uint64_t us)
	{
		stall_time_us = us;
	}

	virtual size_t get_stall_time()
	{
		return stall_time_us;
	}

	virtual void deserialize_lock_status(Decoder &dec)
	{
	}

	virtual void serialize_lock_status(Encoder &enc)
	{
	}

	virtual int32_t get_partition_count() = 0;

	virtual int32_t get_partition(int i) = 0;

	virtual int32_t get_partition_granule_count(int i) = 0;

	virtual int32_t get_granule(int partition_id, int j) = 0;

	virtual bool is_single_partition() = 0;

	virtual const std::string serialize(std::size_t ith_replica = 0) = 0;

	virtual ITable *getTable(size_t tableId, size_t partitionId)
	{
		return get_table(tableId, partitionId);
	}

	void reset()
	{
		pendingResponses = 0;
		network_size = 0;
		abort_lock = false;
		abort_read_validation = false;
                abort_insert = false;
                abort_delete = false;
		local_validated = false;
		si_in_serializable = false;
		distributed_transaction = false;
		execution_phase = true;
		operation.clear();
		readSet.clear();
		writeSet.clear();
                scanSet.clear();
                insertSet.clear();
                deleteSet.clear();
	}

        bool should_abort() {
                return abort_lock || abort_read_validation || abort_insert || abort_delete;
        }

	virtual TransactionResult execute(std::size_t worker_id) = 0;

	virtual void reset_query() = 0;

	template <class KeyType, class ValueType>
	void search_local_index(std::size_t table_id, std::size_t partition_id, const KeyType &key, ValueType &value, bool readonly, std::size_t granule_id = 0)
	{
		TwoPLPashaRWKey readKey;

		readKey.set_table_id(table_id);
		readKey.set_partition_id(partition_id);

		readKey.set_key(&key);
		readKey.set_value(&value);

		readKey.set_local_index_read_bit();
		readKey.set_read_lock_request_bit();

		add_to_read_set(readKey);
	}

	template <class KeyType, class ValueType>
	void search_for_read(std::size_t table_id, std::size_t partition_id, const KeyType &key, ValueType &value, std::size_t granule_id = 0)
	{
		TwoPLPashaRWKey readKey;

		readKey.set_table_id(table_id);
		readKey.set_partition_id(partition_id);

		readKey.set_key(&key);
		readKey.set_value(&value);

		readKey.set_read_lock_request_bit();

		add_to_read_set(readKey);
	}

	template <class KeyType, class ValueType>
	void search_for_update(std::size_t table_id, std::size_t partition_id, const KeyType &key, ValueType &value, std::size_t granule_id = 0)
	{
		TwoPLPashaRWKey readKey;

		readKey.set_table_id(table_id);
		readKey.set_partition_id(partition_id);

		readKey.set_key(&key);
		readKey.set_value(&value);

		readKey.set_write_lock_request_bit();

		add_to_read_set(readKey);
	}

	template <class KeyType, class ValueType>
	void update(std::size_t table_id, std::size_t partition_id, const KeyType &key, const ValueType &value, std::size_t granule_id = 0)
	{
		TwoPLPashaRWKey writeKey;

		writeKey.set_table_id(table_id);
		writeKey.set_partition_id(partition_id);

		writeKey.set_key(&key);
		// the object pointed by value will not be updated
		writeKey.set_value(const_cast<ValueType *>(&value));

		add_to_write_set(writeKey);
	}

        template <class KeyType>
	void scan_for_read(std::size_t table_id, std::size_t partition_id, const KeyType &min_key, const KeyType &max_key,
                        uint64_t limit, void *results, std::size_t granule_id = 0)
	{
		TwoPLPashaRWKey scanKey;

		scanKey.set_table_id(table_id);
		scanKey.set_partition_id(partition_id);

                scanKey.set_scan_args(&min_key, &max_key, limit, results, TwoPLPashaRWKey::SCAN_FOR_READ);

		add_to_scan_set(scanKey);
	}

        template <class KeyType>
	void scan_for_update(std::size_t table_id, std::size_t partition_id, const KeyType &min_key, const KeyType &max_key,
                        uint64_t limit, void *results, std::size_t granule_id = 0)
	{
		TwoPLPashaRWKey scanKey;

		scanKey.set_table_id(table_id);
		scanKey.set_partition_id(partition_id);

                scanKey.set_scan_args(&min_key, &max_key, limit, results, TwoPLPashaRWKey::SCAN_FOR_UPDATE);

		add_to_scan_set(scanKey);
	}

        template <class KeyType>
	void scan_for_insert(std::size_t table_id, std::size_t partition_id, const KeyType &min_key, const KeyType &max_key,
                        uint64_t limit, void *results, std::size_t granule_id = 0)
	{
		TwoPLPashaRWKey scanKey;

		scanKey.set_table_id(table_id);
		scanKey.set_partition_id(partition_id);

                scanKey.set_scan_args(&min_key, &max_key, limit, results, TwoPLPashaRWKey::SCAN_FOR_INSERT);

		add_to_scan_set(scanKey);
	}

        template <class KeyType>
	void scan_for_delete(std::size_t table_id, std::size_t partition_id, const KeyType &min_key, const KeyType &max_key,
                        uint64_t limit, void *results, std::size_t granule_id = 0)
	{
		TwoPLPashaRWKey scanKey;

		scanKey.set_table_id(table_id);
		scanKey.set_partition_id(partition_id);

                scanKey.set_scan_args(&min_key, &max_key, limit, results, TwoPLPashaRWKey::SCAN_FOR_DELETE);

		add_to_scan_set(scanKey);
	}

        /* 
         * The current code does not correctly handle repeated access within a single transaction - repeated read/write, overlapped scan, consecutive insert/delete.
         * This is because we did not implement the ownership of locks so we cannot check if a lock is already held by a transaction.
         * Right now, for consecutive inserts, we only lock the last key.
         */
        template <class KeyType, class ValueType>
	void insert_row(std::size_t table_id, std::size_t partition_id, const KeyType &key, ValueType &value, bool require_lock_next_key, std::size_t granule_id = 0)
	{
		TwoPLPashaRWKey insertKey;

		insertKey.set_table_id(table_id);
		insertKey.set_partition_id(partition_id);

                insertKey.set_key(&key);
                insertKey.set_value(&value);

                if (require_lock_next_key == true)
                        insertKey.set_require_lock_next_row();

		add_to_insert_set(insertKey);
	}

        template <class KeyType>
	void delete_row(std::size_t table_id, std::size_t partition_id, const KeyType &key, std::size_t granule_id = 0)
	{
		TwoPLPashaRWKey deleteKey;

		deleteKey.set_table_id(table_id);
		deleteKey.set_partition_id(partition_id);

                deleteKey.set_key(&key);

		add_to_delete_set(deleteKey);
	}

	bool process_requests(std::size_t worker_id, bool last_call_in_transaction = true)
	{
		bool ret = false;
		ScopedTimer t_local_work([&, this](uint64_t us) { this->record_local_work_time(us); });

		// processing read requests
		for (int i = int(readSet.size()) - 1; i >= 0; i--) {
			// early return
			if (readSet[i].get_processed() == true) {
				break;
			}

			const TwoPLPashaRWKey &readKey = readSet[i];
                        std::tuple<star::ITable::MetaDataType *, void *> cached_row;
			bool success = false, remote = false;
			auto tid = lock_request_handler(readKey.get_table_id(), readKey.get_partition_id(), i, readKey.get_key(), readKey.get_value(),
							readSet[i].get_local_index_read_bit(), readSet[i].get_write_lock_request_bit(), cached_row, success, remote);

			if (!remote) {
                                readSet[i].set_cached_local_row(cached_row);
				if (success) {
					readSet[i].set_tid(tid);
					if (readSet[i].get_read_lock_request_bit() && !readSet[i].get_local_index_read_bit()) {
						readSet[i].set_read_lock_bit();
					}

					if (readSet[i].get_write_lock_request_bit()) {
						readSet[i].set_write_lock_bit();
					}
				} else {
					abort_lock = true;
                                        ret = true;
                                        goto process_net_req_and_ret;
				}
			}
			readSet[i].set_processed();
		}

                // processing scan requests
		for (int i = int(scanSet.size()) - 1; i >= 0; i--) {
                        // early return
			if (scanSet[i].get_processed() == true) {
				break;
			}

			const TwoPLPashaRWKey &scanKey = scanSet[i];
                        ITable::row_entity next_row_entity;
                        bool migration_required = false;
			bool success = scanRequestHandler(scanKey.get_table_id(), scanKey.get_partition_id(), i, scanKey.get_scan_min_key(), scanKey.get_scan_max_key(),
                                        scanKey.get_scan_limit(), scanKey.get_request_type(), scanKey.get_scan_res_vec(), next_row_entity, migration_required);
                        if (migration_required == false) {
                                if (success == false) {
                                        // we fail because of failing to get locks
                                        this->abort_lock = true;
                                        ret = true;
                                        goto process_net_req_and_ret;
                                }
                                // scan succeeds
                                scanSet[i].set_next_row_entity(next_row_entity);
                                scanSet[i].set_next_row_locked();
                        } else {
                                // do nothing
                        }
                        scanSet[i].set_processed();
		}

                // processing insert requests
		for (int i = int(insertSet.size()) - 1; i >= 0; i--) {
                        // early return
			if (insertSet[i].get_processed() == true) {
				break;
			}

			const TwoPLPashaRWKey &insertKey = insertSet[i];
                        ITable::row_entity next_row_entity;
			bool success = insertRequestHandler(insertKey.get_table_id(), insertKey.get_partition_id(), i, insertKey.get_key(), insertKey.get_value(),
                                                        insertKey.get_require_lock_next_row(), next_row_entity);
                        if (success == false) {
                                this->abort_insert = true;
                                ret = true;
                                goto process_net_req_and_ret;
                        }
                        if (insertKey.get_require_lock_next_row() == true) {
                                insertSet[i].set_next_row_entity(next_row_entity);
                                insertSet[i].set_next_row_locked();
                        }
                        insertSet[i].set_processed();
		}

                // processing delete requests
		for (int i = int(deleteSet.size()) - 1; i >= 0; i--) {
                        // early return
			if (deleteSet[i].get_processed() == true) {
				break;
			}

			const TwoPLPashaRWKey &deleteKey = deleteSet[i];
			bool success = deleteRequestHandler(deleteKey.get_table_id(), deleteKey.get_partition_id(), i, deleteKey.get_key());
                        if (success == false) {
                                this->abort_delete = true;
                                ret = true;
                                goto process_net_req_and_ret;
                        }
                        deleteSet[i].set_processed();
		}

process_net_req_and_ret:
		t_local_work.end();
		if (pendingResponses > 0) {
			ScopedTimer t_remote_work([&, this](uint64_t us) { this->record_remote_work_time(us); });
			message_flusher();
			while (pendingResponses > 0) {
				remote_request_handler(0);
			}
		}

                // we may decide to abort after processing some messages
                if (should_abort()) {
                        ret = true;
                }

		return ret;
	}

	TwoPLPashaRWKey *get_read_key(const void *key)
	{
		for (auto i = 0u; i < readSet.size(); i++) {
			if (readSet[i].get_key() == key) {
				return &readSet[i];
			}
		}

		return nullptr;
	}

	std::size_t add_to_read_set(const TwoPLPashaRWKey &key)
	{
		readSet.push_back(key);
		return readSet.size() - 1;
	}

	std::size_t add_to_write_set(const TwoPLPashaRWKey &key)
	{
		writeSet.push_back(key);
		return writeSet.size() - 1;
	}

        std::size_t add_to_scan_set(const TwoPLPashaRWKey &key)
	{
		scanSet.push_back(key);
		return scanSet.size() - 1;
	}

        std::size_t add_to_insert_set(const TwoPLPashaRWKey &key)
	{
		insertSet.push_back(key);
		return insertSet.size() - 1;
	}

        std::size_t add_to_delete_set(const TwoPLPashaRWKey &key)
	{
		deleteSet.push_back(key);
		return deleteSet.size() - 1;
	}

    public:
	std::size_t coordinator_id, partition_id;
	std::chrono::steady_clock::time_point startTime;
	std::size_t pendingResponses;
	std::size_t network_size;
	bool abort_lock, abort_read_validation, abort_insert, abort_delete, local_validated, si_in_serializable;
	bool distributed_transaction;
	bool execution_phase;

	// table id, partition id, key, value, local_index_read?, write_lock?,
	// success?, remote?
	std::function<uint64_t(std::size_t, std::size_t, uint32_t, const void *, void *, bool, bool, std::tuple<star::ITable::MetaDataType *, void *> &, bool &, bool &)> lock_request_handler;
        // table id, partition id, key_offset, min_key, max_key, results
	std::function<bool(std::size_t, std::size_t, uint32_t, const void *, const void *, uint64_t, int, void *, ITable::row_entity &, bool &)> scanRequestHandler;
        // table id, partition id, key_offset, key, value
	std::function<bool(std::size_t, std::size_t, uint32_t, const void *, void *, bool, ITable::row_entity &)> insertRequestHandler;
        // table id, partition id, key_offset, key
	std::function<bool(std::size_t, std::size_t, uint32_t, const void *)> deleteRequestHandler;
	// processed a request?
	std::function<std::size_t(std::size_t id)> remote_request_handler;

	std::function<void()> message_flusher;

	std::function<ITable *(std::size_t, std::size_t)> get_table;

	Partitioner &partitioner;
	std::size_t ith_replica;
	Operation operation;
	std::vector<TwoPLPashaRWKey> readSet, writeSet, scanSet, insertSet, deleteSet;
	WALLogger *logger = nullptr;
	uint64_t txn_random_seed_start = 0;
	uint64_t transaction_id = 0;
	uint64_t straggler_wait_time = 0;

        std::unordered_set<std::size_t> remote_hosts_involved;
};
} // namespace star
