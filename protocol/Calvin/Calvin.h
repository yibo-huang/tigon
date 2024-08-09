//
// Created by Yi Lu on 9/14/18.
//

#pragma once

#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Calvin/CalvinHelper.h"
#include "protocol/Calvin/CalvinMessage.h"
#include "protocol/Calvin/CalvinTransaction.h"

namespace star
{

template <class Database> class Calvin {
    public:
	using DatabaseType = Database;
	using MetaDataType = std::atomic<uint64_t>;
	using ContextType = typename DatabaseType::ContextType;
	using MessageType = CalvinMessage;
	using TransactionType = CalvinTransaction;

	using MessageFactoryType = CalvinMessageFactory;
	using MessageHandlerType = CalvinMessageHandler;

	Calvin(DatabaseType &db, CalvinPartitioner &partitioner)
		: db(db)
		, partitioner(partitioner)
	{
	}

	void abort(std::vector<std::unique_ptr<Message> > &messages, TransactionType &txn, std::size_t lock_manager_id, std::size_t n_lock_manager,
		   std::size_t replica_group_size)
	{
	}

	bool commit(std::vector<std::unique_ptr<Message> > &messages, TransactionType &txn, std::size_t lock_manager_id, std::size_t n_lock_manager,
		    std::size_t replica_group_size)
	{
		ScopedTimer t([&, this](uint64_t us) { txn.record_commit_write_back_time(us); });
		// write to db
		write(messages, txn, lock_manager_id, n_lock_manager, replica_group_size);
		return true;
	}

	void write(std::vector<std::unique_ptr<Message> > &messages, TransactionType &txn, std::size_t lock_manager_id, std::size_t n_lock_manager,
		   std::size_t replica_group_size)
	{
		auto &writeSet = txn.writeSet;
		for (auto i = 0u; i < writeSet.size(); i++) {
			auto &writeKey = writeSet[i];
			auto tableId = writeKey.get_table_id();
			auto partitionId = writeKey.get_partition_id();
			auto table = db.find_table(tableId, partitionId);

			if (partitioner.has_master_partition(partitionId)) {
				auto key = writeKey.get_key();
				auto value = writeKey.get_value();
				table->update(key, value);
			} else {
				auto coordinator_id = partitioner.master_coordinator(partitionId);
				messages[coordinator_id]->set_transaction_id(txn.transaction_id);
				auto sz = MessageFactoryType::new_write_message(*messages[coordinator_id], *table, writeKey.get_key(), writeKey.get_value());
				txn.network_size.fetch_add(sz);
				txn.remote_write++;
			}
		}
	}

    private:
	DatabaseType &db;
	CalvinPartitioner &partitioner;
};
} // namespace star