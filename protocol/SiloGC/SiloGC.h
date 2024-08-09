//
// Created by Yi Lu on 9/10/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>

#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Silo/SiloHelper.h"
#include "protocol/Silo/SiloTransaction.h"
#include "protocol/SiloGC/SiloGCMessage.h"
#include <glog/logging.h>

namespace star
{

template <class Database> class SiloGC {
    public:
	using DatabaseType = Database;
	using MetaDataType = std::atomic<uint64_t>;
	using ContextType = typename DatabaseType::ContextType;
	using MessageType = SiloGCMessage;
	using TransactionType = SiloTransaction;

	using MessageFactoryType = SiloGCMessageFactory;
	using MessageHandlerType = SiloGCMessageHandler;

	SiloGC(DatabaseType &db, const ContextType &context, Partitioner &partitioner)
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

	void abort(TransactionType &txn, std::vector<std::unique_ptr<Message> > &syncMessages, std::vector<std::unique_ptr<Message> > &asyncMessages)
	{
		auto &writeSet = txn.writeSet;

		// unlock locked records

		for (auto i = 0u; i < writeSet.size(); i++) {
			auto &writeKey = writeSet[i];
			// only unlock locked records
			if (!writeKey.get_write_lock_bit())
				continue;
			auto tableId = writeKey.get_table_id();
			auto partitionId = writeKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);
			if (partitioner.has_master_partition(partitionId)) {
				auto key = writeKey.get_key();
				std::atomic<uint64_t> &tid = table->search_metadata(key);
				SiloHelper::unlock(tid);
			} else {
				auto coordinatorID = partitioner.master_coordinator(partitionId);
				txn.network_size += MessageFactoryType::new_abort_message(*syncMessages[coordinatorID], *table, writeKey.get_key());
			}
		}

		sync_messages(txn, false);
	}

	bool commit(TransactionType &txn, std::vector<std::unique_ptr<Message> > &syncMessages, std::vector<std::unique_ptr<Message> > &asyncMessages)
	{
		// lock write set
		if (lock_write_set(txn, syncMessages)) {
			abort(txn, syncMessages, asyncMessages);
			return false;
		}

		// commit phase 2, read validation
		if (!validate_read_set(txn, syncMessages)) {
			abort(txn, syncMessages, asyncMessages);
			return false;
		}

		// generate tid
		uint64_t commit_tid = generate_tid(txn);

		// write and replicate
		write_and_replicate(txn, commit_tid, syncMessages, asyncMessages);

		return true;
	}

    private:
	bool lock_write_set(TransactionType &txn, std::vector<std::unique_ptr<Message> > &messages)
	{
		auto &readSet = txn.readSet;
		auto &writeSet = txn.writeSet;

		for (auto i = 0u; i < writeSet.size(); i++) {
			auto &writeKey = writeSet[i];
			auto tableId = writeKey.get_table_id();
			auto partitionId = writeKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);

			// lock local records
			if (partitioner.has_master_partition(partitionId)) {
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
			} else {
				txn.pendingResponses++;
				auto coordinatorID = partitioner.master_coordinator(partitionId);
				txn.network_size += MessageFactoryType::new_lock_message(*messages[coordinatorID], *table, writeKey.get_key(), i);
			}
		}

		sync_messages(txn);

		return txn.abort_lock;
	}

	bool validate_read_set(TransactionType &txn, std::vector<std::unique_ptr<Message> > &messages)
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

		for (auto i = 0u; i < readSet.size(); i++) {
			auto &readKey = readSet[i];

			if (readKey.get_local_index_read_bit()) {
				continue; // read only index does not need to validate
			}

			bool in_write_set = isKeyInWriteSet(readKey.get_key());
			if (in_write_set) {
				continue; // already validated in lock write set
			}

			auto tableId = readKey.get_table_id();
			auto partitionId = readKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);

			if (partitioner.has_master_partition(partitionId)) {
				auto key = readKey.get_key();
				uint64_t tid = table->search_metadata(key).load();
				if (SiloHelper::remove_lock_bit(tid) != readKey.get_tid()) {
					txn.abort_read_validation = true;
					break;
				}
				if (SiloHelper::is_locked(tid)) { // must be locked by others
					txn.abort_read_validation = true;
					break;
				}
			} else {
				txn.pendingResponses++;
				auto coordinatorID = partitioner.master_coordinator(partitionId);
				txn.network_size += MessageFactoryType::new_read_validation_message(*messages[coordinatorID], *table, readKey.get_key(), i,
												    readKey.get_tid(), false);
			}
		}

		if (txn.pendingResponses == 0) {
			txn.local_validated = true;
		}

		sync_messages(txn);

		return !txn.abort_read_validation;
	}

	uint64_t generate_tid(TransactionType &txn)
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

		for (auto i = 0u; i < writeSet.size(); i++) {
			auto &writeKey = writeSet[i];
			auto tableId = writeKey.get_table_id();
			auto partitionId = writeKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);

			// write
			if (partitioner.has_master_partition(partitionId)) {
				auto key = writeKey.get_key();
				auto value = writeKey.get_value();
				std::atomic<uint64_t> &tid = table->search_metadata(key);
				table->update(key, value);
				SiloHelper::unlock(tid, commit_tid);
			} else {
				auto coordinatorID = partitioner.master_coordinator(partitionId);
				txn.network_size += MessageFactoryType::new_write_message(*syncMessages[coordinatorID], *table, writeKey.get_key(),
											  writeKey.get_value(), commit_tid);
			}

			// value replicate

			std::size_t replicate_count = 0;

			for (auto k = 0u; k < partitioner.total_coordinators(); k++) {
				// k does not have this partition
				if (!partitioner.is_partition_replicated_on(partitionId, k)) {
					continue;
				}

				// already write
				if (k == partitioner.master_coordinator(partitionId)) {
					continue;
				}

				replicate_count++;

				// local replicate
				if (k == txn.coordinator_id) {
					auto key = writeKey.get_key();
					auto value = writeKey.get_value();
					std::atomic<uint64_t> &tid = table->search_metadata(key);

					uint64_t last_tid = SiloHelper::lock(tid);

					if (commit_tid > last_tid) {
						table->update(key, value);
						SiloHelper::unlock(tid, commit_tid);
					} else {
						SiloHelper::unlock(tid);
					}

				} else {
					auto coordinatorID = k;
					txn.network_size += MessageFactoryType::new_replication_message(*asyncMessages[coordinatorID], *table,
													writeKey.get_key(), writeKey.get_value(), commit_tid);
				}
			}

			DCHECK(replicate_count == partitioner.replica_num() - 1);
		}

		sync_messages(txn, false);
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
