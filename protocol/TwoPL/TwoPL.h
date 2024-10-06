//
// Created by Yi Lu on 9/11/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>

#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/TwoPL/TwoPLHelper.h"
#include "protocol/TwoPL/TwoPLMessage.h"
#include "protocol/TwoPL/TwoPLTransaction.h"
#include <glog/logging.h>

namespace star
{

template <class Database> class TwoPL {
    public:
	using DatabaseType = Database;
	using MetaDataType = std::atomic<uint64_t>;
	using ContextType = typename DatabaseType::ContextType;
	using MessageType = TwoPLMessage;
	using TransactionType = TwoPLTransaction;

	using MessageFactoryType = TwoPLMessageFactory;
	using MessageHandlerType = TwoPLMessageHandler;

	TwoPL(DatabaseType &db, const ContextType &context, Partitioner &partitioner)
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
		return TwoPLHelper::read(row, value, value_bytes);
	}

	uint64_t generate_tid(TransactionType &txn)
	{
		auto &readSet = txn.readSet;
		auto &writeSet = txn.writeSet;

		uint64_t next_tid = 0;

		// larger than the TID of any record read or written by the transaction

		for (std::size_t i = 0; i < readSet.size(); i++) {
			next_tid = std::max(next_tid, readSet[i].get_tid());
		}

		// larger than the worker's most recent chosen TID

		next_tid = std::max(next_tid, max_tid);

		// increment

		next_tid++;

		// update worker's most recent chosen TID

		max_tid = next_tid;

		return next_tid;
	}

	void abort(TransactionType &txn, std::vector<std::unique_ptr<Message> > &messages)
	{
                // rollback inserts
                auto &insertSet = txn.insertSet;

                for (auto i = 0u; i < insertSet.size(); i++) {
			auto &insertKey = insertSet[i];

			if (insertKey.get_processed() == false)
				continue;

			auto tableId = insertKey.get_table_id();
			auto partitionId = insertKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);
			if (partitioner.has_master_partition(partitionId)) {
				auto key = insertKey.get_key();
                                bool success = table->remove(key);
                                CHECK(success == true);
			} else {
                                // does not support remote insert & delete
                                CHECK(0);
			}
		}

                // rollback deletes - just release the write locks
                auto &deleteSet = txn.deleteSet;

                for (auto i = 0u; i < deleteSet.size(); i++) {
			auto &deleteKey = deleteSet[i];

			if (deleteKey.get_processed() == false)
				continue;

			auto tableId = deleteKey.get_table_id();
			auto partitionId = deleteKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);
			if (partitioner.has_master_partition(partitionId)) {
                                auto key = deleteKey.get_key();
				std::atomic<uint64_t> &tid = *table->search_metadata(key);
				TwoPLHelper::write_lock_release(tid);
			} else {
                                // does not support remote insert & delete
                                CHECK(0);
			}
		}

		// assume all writes are updates
		auto &readSet = txn.readSet;

		for (auto i = 0u; i < readSet.size(); i++) {
			auto &readKey = readSet[i];
			auto tableId = readKey.get_table_id();
			auto partitionId = readKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);
			if (readKey.get_read_lock_bit()) {
				if (partitioner.has_master_partition(partitionId)) {
					auto key = readKey.get_key();
					auto value = readKey.get_value();
					std::atomic<uint64_t> &tid = *table->search_metadata(key);
					TwoPLHelper::read_lock_release(tid);
				} else {
					auto coordinatorID = partitioner.master_coordinator(partitionId);
					txn.network_size += MessageFactoryType::new_abort_message(*messages[coordinatorID], *table, readKey.get_key(), false);
				}
			}

			if (readKey.get_write_lock_bit()) {
				if (partitioner.has_master_partition(partitionId)) {
					auto key = readKey.get_key();
					auto value = readKey.get_value();
					std::atomic<uint64_t> &tid = *table->search_metadata(key);
					TwoPLHelper::write_lock_release(tid);
				} else {
					auto coordinatorID = partitioner.master_coordinator(partitionId);
					txn.network_size += MessageFactoryType::new_abort_message(*messages[coordinatorID], *table, readKey.get_key(), true);
				}
			}
		}

		sync_messages(txn, false);
	}

	bool commit(TransactionType &txn, std::vector<std::unique_ptr<Message> > &messages)
	{
		if (txn.abort_lock) {
			abort(txn, messages);
			return false;
		}

		{
			ScopedTimer t([&, this](uint64_t us) { txn.record_commit_prepare_time(us); });
			if (txn.get_logger()) {
				prepare_and_redo_for_commit(txn, messages);
			} else {
				prepare_for_commit(txn, messages);
			}
		}

		// all locks are acquired

		uint64_t commit_tid;
		{
			ScopedTimer t([&, this](uint64_t us) { txn.record_local_work_time(us); });
			// generate tid
			commit_tid = generate_tid(txn);
		}

		{
			ScopedTimer t([&, this](uint64_t us) { txn.record_commit_persistence_time(us); });
			// Persist commit record
			if (txn.get_logger()) {
				std::ostringstream ss;
				ss << commit_tid << true;
				auto output = ss.str();
				auto lsn = txn.get_logger()->write(output.c_str(), output.size(), true);
				// txn.get_logger()->sync(lsn, [&](){ txn.remote_request_handler(); });
			}
		}

                // commit inserts
                auto &insertSet = txn.insertSet;
                for (auto i = 0u; i < insertSet.size(); i++) {
			auto &insertKey = insertSet[i];
			CHECK(insertKey.get_processed() == true);

			auto tableId = insertKey.get_table_id();
			auto partitionId = insertKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);
			if (partitioner.has_master_partition(partitionId)) {
				auto key = insertKey.get_key();
                                table->make_placeholder_valid(key);
			} else {
                                // does not support remote insert & delete
                                CHECK(0);
			}
		}

                // commit deletes
                auto &deleteSet = txn.deleteSet;
                for (auto i = 0u; i < deleteSet.size(); i++) {
			auto &deleteKey = deleteSet[i];
			CHECK(deleteKey.get_processed() == true);

			auto tableId = deleteKey.get_table_id();
			auto partitionId = deleteKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);
			if (partitioner.has_master_partition(partitionId)) {
				auto key = deleteKey.get_key();
                                bool success = table->remove(key);
                                CHECK(success == true);
			} else {
                                // does not support remote insert & delete
                                CHECK(0);
			}
		}

		{
			ScopedTimer t([&, this](uint64_t us) { txn.record_commit_write_back_time(us); });
			// write and replicate
			write_and_replicate(txn, commit_tid, messages);
		}

		// release locks
		{
			ScopedTimer t([&, this](uint64_t us) { txn.record_commit_unlock_time(us); });
			// write and replicate
			release_lock(txn, commit_tid, messages);
		}

		return true;
	}

	void write_and_replicate(TransactionType &txn, uint64_t commit_tid, std::vector<std::unique_ptr<Message> > &messages)
	{
		auto &readSet = txn.readSet;
		auto &writeSet = txn.writeSet;

		auto logger = txn.get_logger();
		bool wrote_local_log = false;
		std::vector<bool> persist_commit_record(writeSet.size(), false);
		std::vector<bool> coordinator_covered(this->context.coordinator_num, false);
		std::vector<std::vector<bool> > persist_replication(writeSet.size(), std::vector<bool>(this->context.coordinator_num, false));
		std::vector<bool> coordinator_covered_for_replication(this->context.coordinator_num, false);

		if (txn.get_logger()) {
			// We set persist_commit_record[i] to true if it is the last write to the coordinator
			// We traverse backwards and set the sync flag for the first write whose coordinator_covered is not true
			bool has_other_node = false;
			for (auto i = (int)writeSet.size() - 1; i >= 0; i--) {
				auto &writeKey = writeSet[i];
				auto tableId = writeKey.get_table_id();
				auto partitionId = writeKey.get_partition_id();
				auto table = db.find_table(tableId, partitionId);
				auto key_size = table->key_size();
				auto field_size = table->field_size();
				if (partitioner.has_master_partition(partitionId))
					continue;
				has_other_node = true;
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
			bool has_persist = false;
			for (size_t i = 0; i < writeSet.size(); ++i) {
				if (persist_commit_record[i]) {
					has_persist = true;
				}
			}
			if (writeSet.size() && has_other_node) {
				DCHECK(has_persist);
			}
		}

		for (auto i = 0u; i < writeSet.size(); i++) {
			auto &writeKey = writeSet[i];
			auto tableId = writeKey.get_table_id();
			auto partitionId = writeKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);

			// write
			if (partitioner.has_master_partition(partitionId)) {
				auto key = writeKey.get_key();
				auto value = writeKey.get_value();
				table->update(key, value);
			} else {
				txn.pendingResponses++;
				auto coordinatorID = partitioner.master_coordinator(partitionId);
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
					table->update(key, value);
				} else {
					txn.pendingResponses++;
					auto coordinatorID = k;
					txn.network_size += MessageFactoryType::new_replication_message(*messages[coordinatorID], *table, writeKey.get_key(),
													writeKey.get_value(), commit_tid,
													persist_replication[i][k]);
				}
			}

			DCHECK(replicate_count == partitioner.replica_num() - 1);
		}
		sync_messages(txn);
	}

	void prepare_and_redo_for_commit(TransactionType &txn, std::vector<std::unique_ptr<Message> > &messages)
	{
		auto &readSet = txn.readSet;
		auto &writeSet = txn.writeSet;
		if (txn.is_single_partition()) {
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
				DCHECK(key);
				DCHECK(value);
				std::ostringstream ss;
				ss << tableId << partitionId << key_size << std::string((char *)key, key_size) << value_size
				   << std::string((char *)value, value_size);
				auto output = ss.str();
				txn.get_logger()->write(output.c_str(), output.size(), false);
			}
		} else {
			std::vector<std::vector<TwoPLRWKey> > writeSetGroupByCoordinator(context.coordinator_num);

			for (auto i = 0u; i < writeSet.size(); i++) {
				auto &writeKey = writeSet[i];
				auto tableId = writeKey.get_table_id();
				auto partitionId = writeKey.get_partition_id();
				auto table = db.find_table(tableId, partitionId);
				auto coordinatorId = partitioner.master_coordinator(partitionId);
				writeSetGroupByCoordinator[coordinatorId].push_back(writeKey);
			}

			for (size_t i = 0; i < context.coordinator_num; ++i) {
				auto &writeSet = writeSetGroupByCoordinator[i];
				if (writeSet.empty())
					continue;
				if (i == partitioner.get_coordinator_id()) {
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
						DCHECK(key);
						DCHECK(value);
						std::ostringstream ss;
						ss << tableId << partitionId << key_size << std::string((char *)key, key_size) << value_size
						   << std::string((char *)value, value_size);
						auto output = ss.str();
						txn.get_logger()->write(output.c_str(), output.size(), false);
					}
				} else {
					txn.pendingResponses++;
					auto coordinatorID = i;
					txn.network_size += MessageFactoryType::new_prepare_and_redo_message(*messages[coordinatorID], writeSet, db);
				}
			}
			sync_messages(txn);
		}
	}

	void prepare_for_commit(TransactionType &txn, std::vector<std::unique_ptr<Message> > &messages)
	{
		auto &readSet = txn.readSet;
		auto &writeSet = txn.writeSet;
		std::unordered_set<int> partitions;
		for (auto i = 0u; i < writeSet.size(); i++) {
			auto &writeKey = writeSet[i];
			auto tableId = writeKey.get_table_id();
			auto partitionId = writeKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);

			if (!partitioner.has_master_partition(partitionId)) {
				partitions.insert(partitionId);
			}
		}

		for (auto i = 0u; i < readSet.size(); i++) {
			auto &readKey = readSet[i];
			auto tableId = readKey.get_table_id();
			auto partitionId = readKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);

			if (!partitioner.has_master_partition(partitionId)) {
				partitions.insert(partitionId);
			}
		}

		for (auto it : partitions) {
			auto partitionId = it;
			txn.pendingResponses++;
			auto coordinatorID = partitioner.master_coordinator(partitionId);
			auto table = db.find_table(0, partitionId);
			txn.network_size += MessageFactoryType::new_prepare_message(*messages[coordinatorID], *table);
		}
		sync_messages(txn);
	}

	void release_lock(TransactionType &txn, uint64_t commit_tid, std::vector<std::unique_ptr<Message> > &messages)
	{
		// release read locks
		auto &readSet = txn.readSet;

		for (auto i = 0u; i < readSet.size(); i++) {
			auto &readKey = readSet[i];
			auto tableId = readKey.get_table_id();
			auto partitionId = readKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);
			if (readKey.get_read_lock_bit()) {
				if (partitioner.has_master_partition(partitionId)) {
					auto key = readKey.get_key();
					auto value = readKey.get_value();
					std::atomic<uint64_t> &tid = *table->search_metadata(key);
					TwoPLHelper::read_lock_release(tid);
				} else {
					// txn.pendingResponses++;
					auto coordinatorID = partitioner.master_coordinator(partitionId);
					txn.network_size +=
						MessageFactoryType::new_release_read_lock_message(*messages[coordinatorID], *table, readKey.get_key());
				}
			}
		}

		// release write lock
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
				std::atomic<uint64_t> &tid = *table->search_metadata(key);
				table->update(key, value);
				TwoPLHelper::write_lock_release(tid, commit_tid);
			} else {
				// txn.pendingResponses++;
				auto coordinatorID = partitioner.master_coordinator(partitionId);
				txn.network_size +=
					MessageFactoryType::new_release_write_lock_message(*messages[coordinatorID], *table, writeKey.get_key(), commit_tid);
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