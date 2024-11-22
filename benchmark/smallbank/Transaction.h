//
// Created by Yi Lu on 7/22/18.
//

#pragma once

#include "glog/logging.h"

#include "benchmark/smallbank/Database.h"
#include "benchmark/smallbank/Query.h"
#include "benchmark/smallbank/Schema.h"
#include "benchmark/smallbank/Storage.h"
#include "common/Operation.h"
#include "core/Defs.h"
#include "core/Partitioner.h"
#include "core/Table.h"

namespace star
{
namespace smallbank
{
static thread_local std::vector<Storage *> storage_cache;

Storage *get_storage()
{
	if (storage_cache.empty()) {
		for (size_t i = 0; i < 10; ++i) {
			storage_cache.push_back(new Storage());
		}
	}
	Storage *last = storage_cache.back();
	storage_cache.pop_back();
	return last;
}

void put_storage(Storage *s)
{
	storage_cache.push_back(s);
}

template <class Transaction> class Balance : public Transaction {
    public:
	using DatabaseType = Database;
	using ContextType = typename DatabaseType::ContextType;
	using RandomType = typename DatabaseType::RandomType;
	using StorageType = Storage;

	Balance(std::size_t coordinator_id, std::size_t partition_id, std::size_t granule_id, DatabaseType &db, const ContextType &context,
			RandomType &random, Partitioner &partitioner, std::size_t ith_replica = 0)
		: Transaction(coordinator_id, partition_id, partitioner, ith_replica)
		, db(db)
		, context(context)
		, random(random)
		, partition_id(partition_id)
		, granule_id(granule_id)
		, query(makeBalanceQuery()(context, partition_id, granule_id, random))
	{
		storage = get_storage();
	}

        virtual ~Balance()
	{
		put_storage(storage);
		storage = nullptr;
	}

	virtual int32_t get_partition_count() override
	{
		return query.number_of_parts();
	}

	virtual int32_t get_partition(int ith_partition) override
	{
		return query.get_part(ith_partition);
	}

	virtual int32_t get_partition_granule_count(int ith_partition) override
	{
		return query.get_part_granule_count(ith_partition);
	}

	virtual int32_t get_granule(int ith_partition, int j) override
	{
		return query.get_granule(ith_partition, j);
	}

	virtual bool is_single_partition() override
	{
		return query.number_of_parts() == 1;
	}

	virtual const std::string serialize(std::size_t ith_replica = 0) override
	{
		std::string res;
		Encoder encoder(res);
		encoder << this->transaction_id << this->straggler_wait_time << ith_replica << this->txn_random_seed_start << partition_id << granule_id;
		encoder << get_partition_count();
		// int granules_count = 0;
		// for (int32_t i = 0; i < get_partition_count(); ++i)
		//   granules_count += get_partition_granule_count(i);
		// for (int32_t i = 0; i < get_partition_count(); ++i)
		//   encoder << get_partition(i);
		// encoder << granules_count;
		// for (int32_t i = 0; i < get_partition_count(); ++i)
		//   for (int32_t j = 0; j < get_partition_granule_count(i); ++j)
		//     encoder << get_granule(i, j);
		Transaction::serialize_lock_status(encoder);
		return res;
	}

	TransactionResult execute(std::size_t worker_id) override
	{
                storage->cleanup();
		ScopedTimer t_local_work([&, this](uint64_t us) { this->record_local_work_time(us); });

                // Balance, or Bal(N), is a parameterized transaction that represents calculating the total balance for a customer.
                // It looks up Account to get the CustomerlD value for N, and then returns the sum of savings and checking balances for that CustomerID.

                uint64_t account_id = query.account_id;

                CHECK(context.getPartitionID(account_id) == query.get_part(0));

                int savingsTableID = savings::tableID;
                storage->savings_key.ACCOUNT_ID = account_id;
                this->search_for_read(savingsTableID, context.getPartitionID(account_id), storage->savings_key, storage->savings_value, 0);

                int checkingTableID = checking::tableID;
                storage->checking_key.ACCOUNT_ID = account_id;
                this->search_for_read(checkingTableID, context.getPartitionID(account_id), storage->checking_key, storage->checking_value, 0);

		t_local_work.end();
		if (this->process_requests(worker_id)) {
			return TransactionResult::ABORT;
		}
		t_local_work.reset();

                auto sum = storage->savings_value.BALANCE + storage->checking_value.BALANCE;

		return TransactionResult::READY_TO_COMMIT;
	}

	void reset_query() override
	{
		query = makeBalanceQuery()(context, partition_id, granule_id, random);
	}

    private:
	DatabaseType &db;
	const ContextType &context;
	RandomType random;
	Storage *storage = nullptr;
	std::size_t partition_id, granule_id;
	BalanceQuery query;
};

} // namespace ycsb

} // namespace star
