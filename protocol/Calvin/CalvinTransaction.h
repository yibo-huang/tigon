//
// Created by Yi Lu on 9/14/18.
//

#pragma once

#include "common/Operation.h"
#include "common/WALLogger.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "protocol/Calvin/CalvinHelper.h"
#include "protocol/Calvin/CalvinRWKey.h"
#include <chrono>
#include <glog/logging.h>
#include <thread>

namespace star
{
class CalvinTransaction {
    public:
	using MetaDataType = std::atomic<uint64_t>;

	CalvinTransaction(std::size_t coordinator_id, std::size_t partition_id, Partitioner &partitioner, std::size_t ith_replica)
		: coordinator_id(coordinator_id)
		, partition_id(partition_id)
		, startTime(std::chrono::steady_clock::now())
		, partitioner(partitioner)
		, ith_replica(ith_replica)
	{
		lock_request_for_coordinators.resize(partitioner.total_coordinators());
		reset();
	}

	struct TransactionLockRequest {
		int source_coordinator;
		uint64_t tid;
		std::vector<uint32_t> table_ids;
		std::vector<uint32_t> partition_ids;
		std::vector<bool> read_writes;
		std::vector<CalvinRWKey> keys;
		bool empty()
		{
			return keys.size() == 0;
		}

		void clear()
		{
			table_ids.clear();
			partition_ids.clear();
			read_writes.clear();
			keys.clear();
		}

		void add_key(CalvinRWKey key)
		{
			bool read_or_write = key.get_write_lock_bit();
			keys.push_back(key);
			read_writes.push_back(read_or_write);
			partition_ids.push_back(key.get_partition_id());
			table_ids.push_back(key.get_table_id());
		}
	};

	virtual ~CalvinTransaction() = default;

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
		for (size_t i = 0; i < partitioner.total_coordinators(); ++i) {
			lock_request_for_coordinators[i].clear();
		}
		abort_lock = false;
		abort_no_retry = false;
		abort_read_validation = false;
		local_read.store(0);
		saved_local_read = 0;
		remote_read.store(0);
		saved_remote_read = 0;
		distributed_transaction = false;
		execution_phase = false;
		network_size.store(0);
		active_coordinators.clear();
		operation.clear();
		readSet.clear();
		writeSet.clear();
		async = false;
		processed = false;
	}

	virtual TransactionResult execute(std::size_t worker_id) = 0;

	virtual void reset_query() = 0;

	template <class KeyType, class ValueType>
	void search_local_index(std::size_t table_id, std::size_t partition_id, const KeyType &key, ValueType &value, bool readonly, std::size_t granule_id = 0)
	{
		if (execution_phase) {
			return;
		}
		CalvinRWKey readKey;

		readKey.set_table_id(table_id);
		readKey.set_partition_id(partition_id);

		readKey.set_key(&key);
		readKey.set_value(&value);

		readKey.set_local_index_read_bit();

		add_to_read_set(readKey);
	}

	template <class KeyType, class ValueType>
	void search_for_read(std::size_t table_id, std::size_t partition_id, const KeyType &key, ValueType &value, std::size_t granule_id = 0)
	{
		if (execution_phase) {
			return;
		}

		CalvinRWKey readKey;

		readKey.set_table_id(table_id);
		readKey.set_partition_id(partition_id);

		readKey.set_key(&key);
		readKey.set_value(&value);

		readKey.set_read_lock_bit();

		add_to_read_set(readKey);
	}

	template <class KeyType, class ValueType>
	void search_for_update(std::size_t table_id, std::size_t partition_id, const KeyType &key, ValueType &value, std::size_t granule_id = 0)
	{
		if (execution_phase) {
			return;
		}
		CalvinRWKey readKey;

		readKey.set_table_id(table_id);
		readKey.set_partition_id(partition_id);

		readKey.set_key(&key);
		readKey.set_value(&value);

		readKey.set_write_lock_bit();

		add_to_read_set(readKey);
	}

	template <class KeyType, class ValueType>
	void update(std::size_t table_id, std::size_t partition_id, const KeyType &key, const ValueType &value, std::size_t granule_id = 0)
	{
		if (execution_phase) {
			return;
		}

		CalvinRWKey writeKey;

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

	std::size_t add_to_read_set(const CalvinRWKey &key)
	{
		readSet.push_back(key);
		return readSet.size() - 1;
	}

	std::size_t add_to_write_set(const CalvinRWKey &key)
	{
		writeSet.push_back(key);
		return writeSet.size() - 1;
	}

	void set_id(std::size_t id)
	{
		this->id = id;
	}

	void setup_process_requests_in_prepare_phase()
	{
		// process the reads in read-only index
		// for general reads, increment the local_read and remote_read counter.
		// the function may be called multiple times, the keys are processed in
		// reverse order.
		process_requests_func = [this](std::size_t worker_id) {
			// cannot use unsigned type in reverse iteration
			for (int i = int(readSet.size()) - 1; i >= 0; i--) {
				// early return
				if (readSet[i].get_prepare_processed_bit()) {
					break;
				}

				if (partitioner.has_master_partition(readSet[i].get_partition_id())) {
					local_read.fetch_add(1);
				} else {
					remote_read.fetch_add(1);
				}

				readSet[i].set_prepare_processed_bit();
			}
			return false;
		};
	}

	void setup_process_requests_in_execution_phase(std::size_t n_lock_manager, std::size_t n_worker, std::size_t replica_group_size)
	{
		// only read the keys with locks from the lock_manager_id
		process_requests_func = [this, n_lock_manager, n_worker, replica_group_size](std::size_t worker_id) {
			auto lock_manager_id = CalvinHelper::worker_id_to_lock_manager_id(worker_id, n_lock_manager, n_worker);
			{
				ScopedTimer t_local_work([&, this](uint64_t us) { this->record_local_work_time(us); });

				// cannot use unsigned type in reverse iteration
				for (int i = int(readSet.size()) - 1; i >= 0; i--) {
					// early return
					if (readSet[i].get_execution_processed_bit()) {
						break;
					}

					auto &readKey = readSet[i];
					read_handler(worker_id, readKey.get_table_id(), readKey.get_partition_id(), id, i, readKey.get_key(),
						     readKey.get_value());

					readSet[i].set_execution_processed_bit();
				}
			}

			message_flusher(worker_id);
			if (async == false) {
				ScopedTimer t_remote_work([&, this](uint64_t us) { this->record_remote_work_time(us); });
				DCHECK(local_read.load() == 0);
				while (remote_read.load() > 0) {
					// process remote reads for other workers
					remote_request_handler(worker_id);
				}
			}
			return false;
		};
	}

	void save_read_count()
	{
		saved_local_read = local_read.load();
		saved_remote_read = remote_read.load();
	}

	void load_read_count()
	{
		local_read.store(saved_local_read);
		remote_read.store(saved_remote_read);
	}

	void clear_execution_bit()
	{
		for (auto i = 0u; i < readSet.size(); i++) {
			readSet[i].clear_execution_processed_bit();
		}
	}

	bool process_requests(std::size_t worker_id, bool last_call_in_transaction = true)
	{
		return process_requests_func(worker_id);
	}

    public:
	std::size_t coordinator_id, partition_id, id;
	std::chrono::steady_clock::time_point startTime;
	std::atomic<int32_t> network_size;
	std::atomic<int32_t> local_read{ 0 }, remote_read{ 0 }, remote_write{ 0 };
	int32_t saved_local_read, saved_remote_read;

	bool abort_lock, abort_no_retry, abort_read_validation;
	bool distributed_transaction;
	bool execution_phase;

	std::function<bool(std::size_t)> process_requests_func;

	// table id, partition id, key, value
	std::function<void(std::size_t, std::size_t, const void *, void *)> local_index_read_handler;

	// table id, partition id, id, key_offset, key, value
	std::function<void(std::size_t, std::size_t, std::size_t, std::size_t, uint32_t, const void *, void *)> read_handler;

	// processed a request?
	std::function<std::size_t(std::size_t)> remote_request_handler;

	std::function<void(std::size_t)> message_flusher;

	std::function<ITable *(std::size_t, std::size_t)> get_table;

	Partitioner &partitioner;
	std::size_t ith_replica;
	std::vector<bool> active_coordinators;
	std::vector<TransactionLockRequest> lock_request_for_coordinators;
	std::vector<bool> votes; // votes.size() == lock_request_for_coordinators.size() after LockResponse Phase
	bool processed = false;
	Operation operation; // never used
	std::vector<CalvinRWKey> readSet, writeSet;
	WALLogger *logger = nullptr;
	uint64_t txn_random_seed_start = 0;
	uint64_t transaction_id = 0;
	uint64_t straggler_wait_time = 0;
	bool async = false;
};
} // namespace star