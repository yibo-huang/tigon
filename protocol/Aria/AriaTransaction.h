//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Aria/AriaHelper.h"
#include "protocol/Aria/AriaRWKey.h"
#include <chrono>
#include <glog/logging.h>
#include <thread>

namespace star
{

class AriaTransaction {
    public:
	using MetaDataType = std::atomic<uint64_t>;

	AriaTransaction(std::size_t coordinator_id, std::size_t partition_id, Partitioner &partitioner, std::size_t ith_replica)
		: coordinator_id(coordinator_id)
		, partition_id(partition_id)
		, startTime(std::chrono::steady_clock::now())
		, partitioner(partitioner)
		, ith_replica(ith_replica)
	{
		reset();
	}

	virtual ~AriaTransaction() = default;

	virtual int32_t get_partition_count() = 0;

	virtual int32_t get_partition(int i) = 0;

	virtual int32_t get_partition_granule_count(int i) = 0;

	virtual int32_t get_granule(int partition_id, int j) = 0;

	virtual bool is_single_partition() = 0;

	virtual const std::string serialize(std::size_t ith_replica = 0) = 0;

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

	void reset()
	{
		abort_lock = false;
		abort_no_retry = false;
		abort_read_validation = false;
		distributed_transaction = false;
		execution_phase = false;
		waw = false;
		war = false;
		raw = false;
		pendingResponses = 0;
		network_size = 0;
		operation.clear();
		readSet.clear();
		writeSet.clear();
	}

	virtual TransactionResult execute(std::size_t worker_id) = 0;

	virtual void reset_query() = 0;

	template <class KeyType, class ValueType>
	void search_local_index(std::size_t table_id, std::size_t partition_id, const KeyType &key, ValueType &value, bool readonly, std::size_t granule_id = 0)
	{
		if (execution_phase) {
			return;
		}

		AriaRWKey readKey;

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
		if (execution_phase) {
			return;
		}
		AriaRWKey readKey;

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
		if (execution_phase) {
			return;
		}
		AriaRWKey readKey;

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
		if (execution_phase) {
			return;
		}
		AriaRWKey writeKey;

		writeKey.set_table_id(table_id);
		writeKey.set_partition_id(partition_id);

		writeKey.set_key(&key);
		// the object pointed by value will not be updated
		writeKey.set_value(const_cast<ValueType *>(&value));

		add_to_write_set(writeKey);
	}

	std::size_t add_to_read_set(const AriaRWKey &key)
	{
		readSet.push_back(key);
		return readSet.size() - 1;
	}

	std::size_t add_to_write_set(const AriaRWKey &key)
	{
		writeSet.push_back(key);
		return writeSet.size() - 1;
	}

	void set_id(std::size_t id)
	{
		this->id = id;
	}

	void set_tid_offset(std::size_t offset)
	{
		this->tid_offset = offset;
	}

	void set_epoch(uint32_t epoch)
	{
		this->epoch = epoch;
	}

	bool process_requests(std::size_t worker_id, bool last_call_in_transaction = true)
	{
		ScopedTimer t_local_work([&, this](uint64_t us) { this->record_local_work_time(us); });
		// cannot use unsigned type in reverse iteration
		for (int i = int(readSet.size()) - 1; i >= 0; i--) {
			// early return
			if (!readSet[i].get_read_request_bit()) {
				break;
			}

			AriaRWKey &readKey = readSet[i];
			readRequestHandler(readKey, id, i);
			readSet[i].clear_read_request_bit();
		}

		return false;
	}

	bool is_read_only()
	{
		return writeSet.size() == 0;
	}

    public:
	std::size_t coordinator_id, partition_id, id, tid_offset;
	uint32_t epoch;
	std::chrono::steady_clock::time_point startTime;
	std::size_t pendingResponses;
	std::size_t network_size;

	bool abort_lock, abort_no_retry, abort_read_validation;
	bool distributed_transaction;
	bool execution_phase;
	bool waw, war, raw;

	// read_key, id, key_offset
	std::function<void(AriaRWKey &, std::size_t, std::size_t)> readRequestHandler;

	// processed a request?
	std::function<std::size_t(std::size_t)> remote_request_handler;

	std::function<void()> message_flusher;

	Partitioner &partitioner;
	Operation operation; // never used
	std::vector<AriaRWKey> readSet, writeSet;
	WALLogger *logger = nullptr;
	uint64_t txn_random_seed_start = 0;
	uint64_t transaction_id = 0;
	std::size_t ith_replica;
	uint64_t straggler_wait_time = 0;
	bool aria_aborted = false;
};
} // namespace aria