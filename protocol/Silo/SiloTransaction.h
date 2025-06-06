//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "common/Message.h"
#include "common/WALLogger.h"
#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Silo/SiloRWKey.h"
#include <chrono>
#include <glog/logging.h>
#include <vector>

namespace star
{

class SiloTransaction {
    public:
	using MetaDataType = std::atomic<uint64_t>;

	using DatabaseType = std::atomic<uint64_t>;
	SiloTransaction(std::size_t coordinator_id, std::size_t partition_id, Partitioner &partitioner, std::size_t ith_replica)
		: coordinator_id(coordinator_id)
		, partition_id(partition_id)
		, startTime(std::chrono::steady_clock::now())
		, partitioner(partitioner)
		, ith_replica(ith_replica)
	{
		reset();
	}

	virtual ~SiloTransaction() = default;

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
		local_validated = false;
		si_in_serializable = false;
		distributed_transaction = false;
		execution_phase = true;
		operation.clear();
		readSet.clear();
		writeSet.clear();
	}

        bool should_abort() {
                return abort_lock || abort_read_validation;
        }

	virtual TransactionResult execute(std::size_t worker_id) = 0;

	virtual void reset_query() = 0;

	template <class KeyType, class ValueType>
	void search_local_index(std::size_t table_id, std::size_t partition_id, const KeyType &key, ValueType &value, bool readonly, std::size_t granule_id = 0)
	{
		SiloRWKey readKey;

		readKey.set_table_id(table_id);
		readKey.set_partition_id(partition_id);

		readKey.set_key(&key);
		readKey.set_value(&value);

		readKey.set_local_index_read_bit();
		readKey.set_read_request_bit();

		add_to_read_set(readKey);
	}

	template <class KeyType, class ValueType>
	void search_for_read(std::size_t table_id, std::size_t partition_id, const KeyType &key, ValueType &value, std::size_t granule_id = 0)
	{
		SiloRWKey readKey;

		readKey.set_table_id(table_id);
		readKey.set_partition_id(partition_id);

		readKey.set_key(&key);
		readKey.set_value(&value);

		readKey.set_read_request_bit();

		add_to_read_set(readKey);
	}

	template <class KeyType, class ValueType>
	void search_for_update(std::size_t table_id, std::size_t partition_id, const KeyType &key, ValueType &value, std::size_t granule_id = 0)
	{
		SiloRWKey readKey;

		readKey.set_table_id(table_id);
		readKey.set_partition_id(partition_id);

		readKey.set_key(&key);
		readKey.set_value(&value);

		readKey.set_read_request_bit();

		add_to_read_set(readKey);
	}

	template <class KeyType, class ValueType>
	void update(std::size_t table_id, std::size_t partition_id, const KeyType &key, const ValueType &value, std::size_t granule_id = 0)
	{
		SiloRWKey writeKey;

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
		CHECK(0);
	}

        template <class KeyType>
	void scan_for_update(std::size_t table_id, std::size_t partition_id, const KeyType &min_key, const KeyType &max_key,
                        uint64_t limit, void *results, std::size_t granule_id = 0)
	{
		CHECK(0);
	}

        template <class KeyType>
	void scan_for_insert(std::size_t table_id, std::size_t partition_id, const KeyType &min_key, const KeyType &max_key,
                        uint64_t limit, void *results, std::size_t granule_id = 0)
	{
		CHECK(0);
	}

        template <class KeyType>
	void scan_for_delete(std::size_t table_id, std::size_t partition_id, const KeyType &min_key, const KeyType &max_key,
                        uint64_t limit, void *results, std::size_t granule_id = 0)
	{
		CHECK(0);
	}

        template <class KeyType, class ValueType>
	void insert_row(std::size_t table_id, std::size_t partition_id, const KeyType &key, const ValueType &value, std::size_t granule_id = 0)
	{
		CHECK(0);
	}

        template <class KeyType>
	void delete_row(std::size_t table_id, std::size_t partition_id, const KeyType &key, std::size_t granule_id = 0)
	{
		CHECK(0);
	}

	bool process_requests(std::size_t worker_id, bool last_call_in_transaction = true)
	{
                bool ret = false;
		ScopedTimer t_local_work([&, this](uint64_t us) { this->record_local_work_time(us); });

		// cannot use unsigned type in reverse iteration
		for (int i = int(readSet.size()) - 1; i >= 0; i--) {
			// early return
			if (!readSet[i].get_read_request_bit()) {
				break;
			}

			const SiloRWKey &readKey = readSet[i];
			auto tid = readRequestHandler(readKey.get_table_id(), readKey.get_partition_id(), i, readKey.get_key(), readKey.get_value(),
						      readKey.get_local_index_read_bit());
			readSet[i].clear_read_request_bit();
			readSet[i].set_tid(tid);
		}

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

		return false;
	}

	SiloRWKey *get_read_key(const void *key)
	{
		for (auto i = 0u; i < readSet.size(); i++) {
			if (readSet[i].get_key() == key) {
				return &readSet[i];
			}
		}

		return nullptr;
	}

	std::size_t add_to_read_set(const SiloRWKey &key)
	{
		readSet.push_back(key);
		return readSet.size() - 1;
	}

	std::size_t add_to_write_set(const SiloRWKey &key)
	{
		writeSet.push_back(key);
		return writeSet.size() - 1;
	}

    public:
	std::size_t coordinator_id, partition_id;
	std::chrono::steady_clock::time_point startTime;
	std::size_t pendingResponses;
	std::size_t network_size;
	bool abort_lock, abort_read_validation, local_validated, si_in_serializable;
	bool distributed_transaction;
	bool execution_phase;
	// table id, partition id, key, value, local index read?
	std::function<uint64_t(std::size_t, std::size_t, uint32_t, const void *, void *, bool)> readRequestHandler;
	// processed a request?
	std::function<std::size_t(std::size_t)> remote_request_handler;

	std::function<void()> message_flusher;
	std::function<ITable *(std::size_t, std::size_t)> get_table;

	Partitioner &partitioner;
	std::size_t ith_replica;
	Operation operation;
	std::vector<SiloRWKey> readSet, writeSet;
	WALLogger *logger = nullptr;
	uint64_t txn_random_seed_start = 0;
	uint64_t transaction_id = 0;
	uint64_t straggler_wait_time = 0;
};

} // namespace star
