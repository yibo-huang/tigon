//
// Created by Yi Lu on 8/31/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>
#include <sstream>

#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Silo/SiloHelper.h"
#include "protocol/Silo/SiloMessage.h"
#include "protocol/Silo/SiloTransaction.h"
#include <glog/logging.h>

namespace star
{

template <class Database> class Silo {
    public:
	using DatabaseType = Database;
	using MetaDataType = std::atomic<uint64_t>;
	using ContextType = typename DatabaseType::ContextType;
	using MessageType = SiloMessage;
	using TransactionType = SiloTransaction;

	using MessageFactoryType = SiloMessageFactory;
	using MessageHandlerType = SiloMessageHandler;

	Silo(DatabaseType &db, const ContextType &context, Partitioner &partitioner)
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

	void abort(TransactionType &txn, std::vector<std::unique_ptr<Message> > &messages)
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
				messages[coordinatorID]->set_transaction_id(txn.transaction_id);
				txn.network_size += MessageFactoryType::new_abort_message(*messages[coordinatorID], *table, writeKey.get_key());
			}
		}

		sync_messages(txn, false);
	}

	bool commit(TransactionType &txn, std::vector<std::unique_ptr<Message> > &messages)
	{
		{
			ScopedTimer t([&, this](uint64_t us) { txn.record_commit_prepare_time(us); });
			// lock write set
			if (lock_write_set(txn, messages)) {
				abort(txn, messages);
				return false;
			}

			if (txn.get_logger()) {
				// commit phase 2, read validation and redo
				if (!validate_read_set_and_redo(txn, messages)) {
					abort(txn, messages);
					return false;
				}
			} else {
				// commit phase 2, read validation
				if (!validate_read_set(txn, messages)) {
					abort(txn, messages);
					return false;
				}
			}
		}

		// generate tid
		uint64_t commit_tid;
		{
			ScopedTimer t([&, this](uint64_t us) { txn.record_local_work_time(us); });
			commit_tid = generate_tid(txn);
		}

		{
			ScopedTimer t([&, this](uint64_t us) { txn.record_commit_persistence_time(us); });
			// Passed validation, persist commit record
			if (txn.get_logger()) {
				std::ostringstream ss;
				ss << commit_tid << true;
				auto output = ss.str();
				auto lsn = txn.get_logger()->write(output.c_str(), output.size(), true);
			}
		}

		// write and replicate
		{
			ScopedTimer t([&, this](uint64_t us) { txn.record_commit_write_back_time(us); });
			write_and_replicate(txn, commit_tid, messages);
		}

		// release locks
		{
			ScopedTimer t([&, this](uint64_t us) { txn.record_commit_unlock_time(us); });
			release_lock(txn, commit_tid, messages);
		}

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
				writeKey.set_tid(latestTid);

				auto readKeyPtr = txn.get_read_key(key);
				// assume no blind write
				DCHECK(readKeyPtr != nullptr);
				uint64_t tidOnRead = readKeyPtr->get_tid();
				if (latestTid != tidOnRead) {
					txn.abort_lock = true;
					break;
				}

			} else {
				txn.pendingResponses++;
				auto coordinatorID = partitioner.master_coordinator(partitionId);
				messages[coordinatorID]->set_transaction_id(txn.transaction_id);
				txn.network_size += MessageFactoryType::new_lock_message(*messages[coordinatorID], *table, writeKey.get_key(), i);
			}
		}

		sync_messages(txn);

		return txn.abort_lock;
	}

	bool validate_read_set_and_redo(TransactionType &txn, std::vector<std::unique_ptr<Message> > &messages)
	{
		DCHECK(txn.get_logger());
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

		if (txn.is_single_partition()) {
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
				auto key = readKey.get_key();
				auto tid = readKey.get_tid();

				DCHECK(partitioner.has_master_partition(partitionId));

				uint64_t latest_tid = table->search_metadata(key).load();
				if (SiloHelper::remove_lock_bit(latest_tid) != tid) {
					txn.abort_read_validation = true;
					break;
				}
				if (SiloHelper::is_locked(latest_tid)) { // must be locked by others
					txn.abort_read_validation = true;
					break;
				}
			}

			DCHECK(txn.pendingResponses == 0);

			// Redo logging
			for (size_t j = 0; j < writeSet.size(); ++j) {
				auto &writeKey = writeSet[j];
				auto tableId = writeKey.get_table_id();
				auto partitionId = writeKey.get_partition_id();
				auto table = db.find_table(tableId, partitionId);
				auto key_size = table->key_size();
				auto value_size = table->value_size();
				auto key = writeKey.get_key();
				auto value = writeKey.get_value();

				std::ostringstream ss;
				ss << tableId << partitionId << key_size << std::string((char *)key, key_size) << value_size
				   << std::string((char *)value, value_size);
				auto output = ss.str();
				txn.get_logger()->write(output.c_str(), output.size(), false);
			}
		} else {
			std::vector<std::vector<SiloRWKey> > readSetGroupByCoordinator(context.coordinator_num);
			std::vector<std::vector<SiloRWKey> > writeSetGroupByCoordinator(context.coordinator_num);

			for (auto i = 0u; i < readSet.size(); ++i) {
				auto &readKey = readSet[i];
				if (readKey.get_local_index_read_bit()) {
					continue; // read only index does not need to validate
				}
				bool in_write_set = isKeyInWriteSet(readKey.get_key());
				if (in_write_set) {
					continue; // already validated in lock write set
				}
				auto partitionId = readKey.get_partition_id();
				auto coordinatorId = partitioner.master_coordinator(partitionId);
				readSetGroupByCoordinator[coordinatorId].push_back(readKey);
			}

			for (auto i = 0u; i < writeSet.size(); ++i) {
				auto &writeKey = writeSet[i];
				auto partitionId = writeKey.get_partition_id();
				auto coordinatorId = partitioner.master_coordinator(partitionId);
				writeSetGroupByCoordinator[coordinatorId].push_back(writeKey);
			}

			for (size_t i = 0; i < context.coordinator_num && txn.abort_read_validation == false; ++i) {
				auto &readSet = readSetGroupByCoordinator[i];
				auto &writeSet = writeSetGroupByCoordinator[i];
				if (i == partitioner.get_coordinator_id()) {
					for (size_t j = 0; j < readSet.size(); ++j) {
						auto &readKey = readSet[j];
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
						auto key = readKey.get_key();
						auto tid = readKey.get_tid();

						uint64_t latest_tid = table->search_metadata(key).load();
						if (SiloHelper::remove_lock_bit(latest_tid) != tid) {
							txn.abort_read_validation = true;
							break;
						}
						if (SiloHelper::is_locked(latest_tid)) { // must be locked by others
							txn.abort_read_validation = true;
							break;
						}
					}

					// Redo logging
					for (size_t j = 0; j < writeSet.size(); ++j) {
						auto &writeKey = writeSet[j];
						auto tableId = writeKey.get_table_id();
						auto partitionId = writeKey.get_partition_id();
						auto table = db.find_table(tableId, partitionId);
						auto key_size = table->key_size();
						auto value_size = table->value_size();
						auto key = writeKey.get_key();
						auto value = writeKey.get_value();

						std::ostringstream ss;
						ss << tableId << partitionId << key_size << std::string((char *)key, key_size) << value_size
						   << std::string((char *)value, value_size);
						auto output = ss.str();
						txn.get_logger()->write(output.c_str(), output.size(), false);
					}

				} else {
					if (readSet.empty() && writeSet.empty())
						continue;
					txn.pendingResponses++;
					auto coordinatorID = i;
					messages[coordinatorID]->set_transaction_id(txn.transaction_id);
					txn.network_size +=
						MessageFactoryType::new_read_validation_and_redo_message(*messages[coordinatorID], readSet, writeSet, db);
				}
			}

			if (txn.pendingResponses == 0) {
				txn.local_validated = true;
			}

			sync_messages(txn);
		}

		return !txn.abort_read_validation;
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
			auto key = readKey.get_key();
			auto tid = readKey.get_tid();

			if (partitioner.has_master_partition(partitionId)) {
				uint64_t latest_tid = table->search_metadata(key).load();
				if (SiloHelper::remove_lock_bit(latest_tid) != tid) {
					txn.abort_read_validation = true;
					break;
				}
				if (SiloHelper::is_locked(latest_tid)) { // must be locked by others
					txn.abort_read_validation = true;
					break;
				}
			} else {
				txn.pendingResponses++;
				auto coordinatorID = partitioner.master_coordinator(partitionId);
				messages[coordinatorID]->set_transaction_id(txn.transaction_id);
				txn.network_size += MessageFactoryType::new_read_validation_message(*messages[coordinatorID], *table, key, i, tid);
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

	void write_and_replicate(TransactionType &txn, uint64_t commit_tid, std::vector<std::unique_ptr<Message> > &messages)
	{
		auto &readSet = txn.readSet;
		auto &writeSet = txn.writeSet;
		auto logger = txn.get_logger();
		std::vector<bool> persist_commit_record(writeSet.size(), false);
		std::vector<bool> coordinator_covered(this->context.coordinator_num, false);
		std::vector<std::vector<bool> > persist_replication(writeSet.size(), std::vector<bool>(this->context.coordinator_num, false));
		std::vector<bool> coordinator_covered_for_replication(this->context.coordinator_num, false);

		if (txn.get_logger()) {
			// We set persist_commit_record[i] to true if it is the last write to the coordinator
			// We traverse backwards and set the sync flag for the first write whose coordinator_covered is not true
			for (auto i = (int)writeSet.size() - 1; i >= 0; i--) {
				auto &writeKey = writeSet[i];
				auto tableId = writeKey.get_table_id();
				auto partitionId = writeKey.get_partition_id();
				auto table = db.find_table(tableId, partitionId);
				auto key_size = table->key_size();
				auto field_size = table->field_size();
				if (partitioner.has_master_partition(partitionId))
					continue;
				auto coordinatorId = partitioner.master_coordinator(partitionId);
				if (coordinator_covered[coordinatorId] == false) {
					coordinator_covered[coordinatorId] = true;
					persist_commit_record[i] = true;
				}

				for (auto k = 0u; k < partitioner.total_coordinators(); ++k) {
					// k does not have this partition
					if (!partitioner.is_partition_replicated_on(partitionId, k)) {
						continue;
					}

					// already write
					if (k == partitioner.master_coordinator(partitionId)) {
						continue;
					}

					// remote replication
					if (k != txn.coordinator_id && coordinator_covered_for_replication[k] == false) {
						coordinator_covered_for_replication[k] = true;
						persist_replication[i][k] = true;
					}
				}
			}
		}

		for (auto i = 0u; i < writeSet.size(); i++) {
			auto &writeKey = writeSet[i];
			auto tableId = writeKey.get_table_id();
			auto partitionId = writeKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);
			auto key_size = table->key_size();
			auto field_size = table->field_size();

			// write
			if (partitioner.has_master_partition(partitionId)) {
				auto key = writeKey.get_key();
				auto value = writeKey.get_value();
				table->update(key, value);
			} else {
				txn.pendingResponses++;
				auto coordinatorID = partitioner.master_coordinator(partitionId);
				messages[coordinatorID]->set_transaction_id(txn.transaction_id);
				txn.network_size += MessageFactoryType::new_write_message(*messages[coordinatorID], *table, writeKey.get_key(),
											  writeKey.get_value(), commit_tid, persist_commit_record[i]);
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
					DCHECK(last_tid < commit_tid);
					table->update(key, value);
					SiloHelper::unlock(tid, commit_tid);

				} else {
					txn.pendingResponses++;
					auto coordinatorID = k;
					messages[coordinatorID]->set_transaction_id(txn.transaction_id);
					txn.network_size += MessageFactoryType::new_replication_message(*messages[coordinatorID], *table, writeKey.get_key(),
													writeKey.get_value(), commit_tid,
													persist_replication[i][k]);
				}
			}

			DCHECK(replicate_count == partitioner.replica_num() - 1);
		}
		sync_messages(txn);
	}

	void release_lock(TransactionType &txn, uint64_t commit_tid, std::vector<std::unique_ptr<Message> > &messages)
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
				messages[coordinatorID]->set_transaction_id(txn.transaction_id);
				txn.network_size +=
					MessageFactoryType::new_release_lock_message(*messages[coordinatorID], *table, writeKey.get_key(), commit_tid);
			}
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
