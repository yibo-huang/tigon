//
// Created by Xinjing on 9/12/21.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>

#include "common/Percentile.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/H-Store/HStoreHelper.h"
#include "protocol/H-Store/HStoreMessage.h"
#include "protocol/H-Store/HStoreTransaction.h"
#include <glog/logging.h>

namespace star
{

template <class Database> class HStore {
    public:
	using DatabaseType = Database;
	using MetaDataType = std::atomic<uint64_t>;
	using ContextType = typename DatabaseType::ContextType;
	using MessageType = TwoPLMessage;
	using TransactionType = HStoreTransaction;

	using MessageFactoryType = HStoreMessageFactory;
	using MessageHandlerType = HStoreMessageHandler;

	HStore(DatabaseType &db, const ContextType &context, Partitioner &partitioner)
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
		HStoreHelper::read(row, value, value_bytes);
		return 0;
	}

	uint64_t generate_tid(TransactionType &txn)
	{
		static std::atomic<uint64_t> tid_counter{ 1 };
		return tid_counter.fetch_add(1);
	}

	void abort(TransactionType &txn, std::vector<std::unique_ptr<Message> > &messages)
	{
		// assume all writes are updates
		if (!txn.is_single_partition()) {
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
						std::atomic<uint64_t> &tid = table->search_metadata(key);
						TwoPLHelper::read_lock_release(tid);
					} else {
						auto coordinatorID = partitioner.master_coordinator(partitionId);
						txn.network_size +=
							MessageFactoryType::new_abort_message(*messages[coordinatorID], *table, readKey.get_key(), false);
					}
				}

				if (readKey.get_write_lock_bit()) {
					if (partitioner.has_master_partition(partitionId)) {
						auto key = readKey.get_key();
						auto value = readKey.get_value();
						std::atomic<uint64_t> &tid = table->search_metadata(key);
						TwoPLHelper::write_lock_release(tid);
					} else {
						auto coordinatorID = partitioner.master_coordinator(partitionId);
						txn.network_size +=
							MessageFactoryType::new_abort_message(*messages[coordinatorID], *table, readKey.get_key(), true);
					}
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

		// all locks are acquired

		// generate tid
		uint64_t commit_tid = generate_tid(txn);

		// write and replicate
		write_and_replicate(txn, commit_tid, messages);

		// release locks
		release_lock(txn, commit_tid, messages);

		return true;
	}

	void write_and_replicate(TransactionType &txn, uint64_t commit_tid, std::vector<std::unique_ptr<Message> > &messages)
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
				table->update(key, value);
			} else {
				txn.pendingResponses++;
				auto coordinatorID = partitioner.master_coordinator(partitionId);
				txn.network_size +=
					MessageFactoryType::new_write_message(*messages[coordinatorID], *table, writeKey.get_key(), writeKey.get_value());
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
													writeKey.get_value(), commit_tid);
				}
			}

			DCHECK(replicate_count == partitioner.replica_num() - 1);
		}
		sync_messages(txn);
	}

	void release_lock(TransactionType &txn, uint64_t commit_tid, std::vector<std::unique_ptr<Message> > &messages)
	{
		if (txn.is_single_partition()) {
			// For single-partition transactions, do nothing.
			// release single partition lock

		} else {
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
						std::atomic<uint64_t> &tid = table->search_metadata(key);
						TwoPLHelper::read_lock_release(tid);
					} else {
						txn.pendingResponses++;
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
					std::atomic<uint64_t> &tid = table->search_metadata(key);
					table->update(key, value);
					TwoPLHelper::write_lock_release(tid, commit_tid);
				} else {
					txn.pendingResponses++;
					auto coordinatorID = partitioner.master_coordinator(partitionId);
					txn.network_size += MessageFactoryType::new_release_write_lock_message(*messages[coordinatorID], *table,
													       writeKey.get_key(), commit_tid);
				}
			}
		}

		sync_messages(txn);
	}

	void sync_messages(TransactionType &txn, bool wait_response = true)
	{
		txn.message_flusher();
		if (wait_response) {
			// LOG(INFO) << "Waiting for " << txn.pendingResponses << " responses";
			while (txn.pendingResponses > 0) {
				txn.remote_request_handler(0);
			}
		}
	}

	void report_sync_latency_percentile();

    private:
	DatabaseType &db;
	const ContextType &context;
	Partitioner &partitioner;
	uint64_t max_tid = 0;
};
} // namespace star