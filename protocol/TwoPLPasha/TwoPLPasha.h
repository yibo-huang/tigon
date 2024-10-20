//
// Created by Yi Lu on 9/11/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>

#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/TwoPLPasha/TwoPLPashaHelper.h"
#include "protocol/TwoPLPasha/TwoPLPashaMessage.h"
#include "protocol/TwoPLPasha/TwoPLPashaTransaction.h"
#include <glog/logging.h>

namespace star
{

template <class Database> class TwoPLPasha {
    public:
	using DatabaseType = Database;
	using MetaDataType = std::atomic<uint64_t>;
	using ContextType = typename DatabaseType::ContextType;
	using MessageType = TwoPLPashaMessage;
	using TransactionType = TwoPLPashaTransaction;

	using MessageFactoryType = TwoPLPashaMessageFactory;
	using MessageHandlerType = TwoPLPashaMessageHandler;

	TwoPLPasha(DatabaseType &db, const ContextType &context, Partitioner &partitioner)
		: db(db)
		, context(context)
		, partitioner(partitioner)
	{
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
				std::atomic<uint64_t> *meta = table->search_metadata(key);
                                CHECK(meta != 0);
				TwoPLPashaHelper::write_lock_release(*meta);
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
					std::atomic<uint64_t> *meta = table->search_metadata(key);
                                        CHECK(meta != 0);
					TwoPLPashaHelper::read_lock_release(*meta);
				} else {
					auto key = readKey.get_key();
                                        char *migrated_row = twopl_pasha_global_helper->get_migrated_row(tableId, partitionId, key, false);
                                        CHECK(migrated_row != nullptr);
                                        TwoPLPashaHelper::remote_read_lock_release(migrated_row);
				}
			}

			if (readKey.get_write_lock_bit()) {
				if (partitioner.has_master_partition(partitionId)) {
					auto key = readKey.get_key();
					auto value = readKey.get_value();
					std::atomic<uint64_t> *meta = table->search_metadata(key);
                                        CHECK(meta != 0);
					TwoPLPashaHelper::write_lock_release(*meta);
				} else {
					auto key = readKey.get_key();
                                        char *migrated_row = twopl_pasha_global_helper->get_migrated_row(tableId, partitionId, key, false);
                                        CHECK(migrated_row != nullptr);
                                        TwoPLPashaHelper::remote_write_lock_release(migrated_row);
				}
			}
		}

                // release migrated rows
                release_migrated_rows(txn);

                // reactively move out data
                if (migration_manager->when_to_move_out == MigrationManager::Reactive) {
                        // send out data move out hints
                        for (auto remote_host_id : txn.remote_hosts_involved) {
                                txn.network_size += MessageFactoryType::new_data_move_out_hint_message(*messages[remote_host_id]);
                        }
                }
	}

	bool commit(TransactionType &txn, std::vector<std::unique_ptr<Message> > &messages)
	{
		if (txn.abort_lock) {
			abort(txn, messages);
			return false;
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
                                std::atomic<uint64_t> *meta = table->search_metadata(key);
                                CHECK(meta != 0);
                                TwoPLPashaHelper::mark_tuple_as_valid(*meta);
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

                // release migrated rows
                release_migrated_rows(txn);

                // reactively move out data
                if (migration_manager->when_to_move_out == MigrationManager::Reactive) {
                        // send out data move out hints
                        for (auto remote_host_id : txn.remote_hosts_involved) {
                                txn.network_size += MessageFactoryType::new_data_move_out_hint_message(*messages[remote_host_id]);
                        }
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

                                // replication not supported
                                DCHECK(false);

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
                                auto value_size = table->value_size();
				auto row = table->search(key);
                                twopl_pasha_global_helper->update(row, value, value_size);
			} else {
				auto key = writeKey.get_key();
				auto value = writeKey.get_value();
                                auto value_size = table->value_size();
				char *migrated_row = twopl_pasha_global_helper->get_migrated_row(tableId, partitionId, key, false);
                                CHECK(migrated_row != nullptr);
                                twopl_pasha_global_helper->remote_update(migrated_row, value, value_size);
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
					auto value_size = table->value_size();
                                        auto row = table->search(key);
                                        twopl_pasha_global_helper->update(row, value, value_size);
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
					std::atomic<uint64_t> *meta = table->search_metadata(key);
                                        CHECK(meta != 0);
					TwoPLPashaHelper::read_lock_release(*meta);
				} else {
					auto key = readKey.get_key();
					char *migrated_row = twopl_pasha_global_helper->get_migrated_row(tableId, partitionId, key, false);
                                        CHECK(migrated_row != nullptr);
                                        twopl_pasha_global_helper->remote_read_lock_release(migrated_row);
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
				std::atomic<uint64_t> *meta = table->search_metadata(key);
                                CHECK(meta != nullptr);
				TwoPLPashaHelper::write_lock_release(*meta, commit_tid);
			} else {
				auto key = writeKey.get_key();
                                char *migrated_row = twopl_pasha_global_helper->get_migrated_row(tableId, partitionId, key, false);
                                CHECK(migrated_row != nullptr);
                                twopl_pasha_global_helper->remote_write_lock_release(migrated_row, commit_tid);
			}
		}
	}

        void release_migrated_rows(TransactionType &txn)
        {
                auto &readSet = txn.readSet;

                // release rows that are read but not written
		for (auto i = 0u; i < readSet.size(); ++i) {
			auto &readKey = readSet[i];
                        auto tableId = readKey.get_table_id();
			auto partitionId = readKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);

			if (readKey.get_reference_counted() == true) {
                                DCHECK(partitioner.has_master_partition(partitionId) == false);
                                auto key = readKey.get_key();
                                twopl_pasha_global_helper->release_migrated_row(tableId, partitionId, key);
                        }
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
