//
// Created by Yi Lu on 9/6/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>

#include "core/ControlMessage.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "core/Worker.h"
#include "protocol/Silo/SiloHelper.h"
#include "protocol/Silo/SiloRWKey.h"
#include "protocol/Silo/SiloTransaction.h"
#include "protocol/Star/StarManager.h"
#include "protocol/Star/StarMessage.h"
#include <glog/logging.h>

namespace star
{

template <class Database> class Star {
    public:
	using DatabaseType = Database;
	using MetaDataType = std::atomic<uint64_t>;
	using ContextType = typename DatabaseType::ContextType;
	using MessageType = StarMessage;
	using TransactionType = SiloTransaction;

	using MessageFactoryType = StarMessageFactory;
	using MessageHandlerType = StarMessageHandler<DatabaseType>;

	Star(DatabaseType &db, const ContextType &context, Partitioner &partitioner)
		: db(db)
		, context(context)
		, partitioner(partitioner)
	{
	}

	uint64_t search(std::size_t table_id, std::size_t partition_id, const void *key, void *value) const
	{
		ITable *table = db.find_table(table_id, partition_id);
		auto value_bytes = table->value_size();
		auto row = table->search(key);
		return SiloHelper::read(row, value, value_bytes);
	}

	void abort(TransactionType &txn)
	{
		auto &writeSet = txn.writeSet;
		// unlock locked records
		for (auto i = 0u; i < writeSet.size(); i++) {
			auto &writeKey = writeSet[i];

			auto tableId = writeKey.get_table_id();
			auto partitionId = writeKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);
			auto key = writeKey.get_key();
			std::atomic<uint64_t> &tid = table->search_metadata(key);
			SiloHelper::unlock(tid);
		}
	}

	bool commit(TransactionType &txn, std::vector<std::unique_ptr<Message> > &syncMessages, std::vector<std::unique_ptr<Message> > &asyncMessages)
	{
		// lock write set
		if (lock_write_set(txn)) {
			abort(txn);
			return false;
		}
		// commit phase 2, read validation
		if (!validate_read_set(txn)) {
			abort(txn);
			return false;
		}

		// generate tid
		uint64_t commit_tid = generateTid(txn);

		// write and replicate
		write_and_replicate(txn, commit_tid, syncMessages, asyncMessages);

		return true;
	}

    private:
	bool lock_write_set(TransactionType &txn)
	{
		auto &readSet = txn.readSet;
		auto &writeSet = txn.writeSet;

		for (auto i = 0u; i < writeSet.size(); i++) {
			auto &writeKey = writeSet[i];
			auto tableId = writeKey.get_table_id();
			auto partitionId = writeKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);

			auto key = writeKey.get_key();
			std::atomic<uint64_t> &tid = table->search_metadata(key);

			bool success;
			uint64_t latestTid = SiloHelper::lock(tid, success);

			if (!success) {
				txn.abort_lock = true;
				break;
			}

			writeKey.set_write_lock_bit();

			auto readKeyPtr = txn.get_read_key(key);
			// assume no blind write
			DCHECK(readKeyPtr != nullptr);
			uint64_t tidOnRead = readKeyPtr->get_tid();
			if (latestTid != tidOnRead) {
				txn.abort_lock = true;
				break;
			}

			writeKey.set_tid(latestTid);
		}

		return txn.abort_lock;
	}

	bool validate_read_set(TransactionType &txn)
	{
		auto &readSet = txn.readSet;
		auto &writeSet = txn.writeSet;

		auto isKeyInWriteSet = [&writeSet](const void *key) {
			for (auto &writeKey : writeSet) {
				if (writeKey.get_key() == key) {
					return true;
				}
			}
			return false;
		};

		for (auto &readKey : readSet) {
			bool in_write_set = isKeyInWriteSet(readKey.get_key());
			if (in_write_set)
				continue; // already validated in lock write set

			auto tableId = readKey.get_table_id();
			auto partitionId = readKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);
			auto key = readKey.get_key();
			uint64_t tid = table->search_metadata(key).load();

			if (SiloHelper::remove_lock_bit(tid) != readKey.get_tid()) {
				txn.abort_read_validation = true;
				return false;
			}

			if (SiloHelper::is_locked(tid)) { // must be locked by others
				txn.abort_read_validation = true;
				return false;
			}
		}
		return true;
	}

	uint64_t generateTid(TransactionType &txn)
	{
		auto &readSet = txn.readSet;
		auto &writeSet = txn.writeSet;

		uint64_t next_tid = 0;

		/*
		 *  A timestamp is a 64-bit word.
		 *  The most significant bit is the lock bit.
		 *  The lower 63 bits are for transaction sequence id.
		 *  [  lock bit (1)  |  id (63) ]
		 */

		// larger than the TID of any record read or written by the transaction

		for (std::size_t i = 0; i < readSet.size(); i++) {
			next_tid = std::max(next_tid, readSet[i].get_tid());
		}

		for (std::size_t i = 0; i < writeSet.size(); i++) {
			next_tid = std::max(next_tid, writeSet[i].get_tid());
		}

		// larger than the worker's most recent chosen TID

		next_tid = std::max(next_tid, max_tid);

		// increment

		next_tid++;

		// update worker's most recent chosen TID

		max_tid = next_tid;

		return next_tid;
	}

	void write_and_replicate(TransactionType &txn, uint64_t commit_tid, std::vector<std::unique_ptr<Message> > &syncMessages,
				 std::vector<std::unique_ptr<Message> > &asyncMessages)
	{
		auto &readSet = txn.readSet;
		auto &writeSet = txn.writeSet;

		std::vector<std::atomic<uint64_t> *> tids;

		// write to local db

		for (auto i = 0u; i < writeSet.size(); i++) {
			auto &writeKey = writeSet[i];
			auto tableId = writeKey.get_table_id();
			auto partitionId = writeKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);

			auto key = writeKey.get_key();
			auto value = writeKey.get_value();

			std::atomic<uint64_t> &tid = table->search_metadata(key);
			tids.push_back(&tid);
			table->update(key, value);
		}

		// replicate to remote db

		// operation replication optimization in the partitioned phase
		if (context.operation_replication) {
			txn.operation.set_tid(commit_tid);
			auto partition_id = txn.operation.partition_id;

			for (auto k = 0u; k < partitioner.total_coordinators(); k++) {
				// k does not have this partition
				if (!partitioner.is_partition_replicated_on(partition_id, k)) {
					continue;
				}
				// already write
				if (k == txn.coordinator_id) {
					continue;
				}

				txn.network_size += MessageFactoryType::new_operation_replication_message(*asyncMessages[k], txn.operation);
			}
		} else {
			// value replication
			for (auto i = 0u; i < writeSet.size(); i++) {
				auto &writeKey = writeSet[i];
				auto tableId = writeKey.get_table_id();
				auto partitionId = writeKey.get_partition_id();
				auto table = db.find_table(tableId, partitionId);

				auto key = writeKey.get_key();
				auto value = writeKey.get_value();

				// value replicate
				for (auto k = 0u; k < partitioner.total_coordinators(); k++) {
					// k does not have this partition
					if (!partitioner.is_partition_replicated_on(partitionId, k)) {
						continue;
					}

					// already write
					if (k == txn.coordinator_id) {
						continue;
					}
					if (context.star_sync_in_single_master_phase) {
						txn.pendingResponses++;
						txn.network_size += MessageFactoryType::new_sync_value_replication_message(*syncMessages[k], *table, key, value,
															   commit_tid);
					} else {
						txn.network_size += MessageFactoryType::new_async_value_replication_message(*asyncMessages[k], *table, key,
															    value, commit_tid);
					}
				}
			}
		}

		if (context.star_sync_in_single_master_phase) {
			sync_messages(txn);
		}

		for (auto i = 0u; i < tids.size(); i++) {
			SiloHelper::unlock(*tids[i], commit_tid);
		}
	}

	void sync_messages(TransactionType &txn, bool wait_response = true)
	{
		txn.message_flusher();
		if (wait_response) {
			while (txn.pendingResponses > 0) {
				txn.remote_request_handler(0);
			}
		}
	}

    private:
	DatabaseType &db;
	const ContextType &context;
	Partitioner &partitioner;
	uint64_t max_tid = 0;
};

} // namespace star
