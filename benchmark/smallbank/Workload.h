//
// Created by Yi Lu on 7/25/18.
//

#pragma once

#include "benchmark/smallbank/Context.h"
#include "benchmark/smallbank/Database.h"
#include "benchmark/smallbank/Random.h"
#include "benchmark/smallbank/Storage.h"
#include "benchmark/smallbank/Transaction.h"

namespace star
{

namespace smallbank
{

template <class Transaction> class Workload {
    public:
	using TransactionType = Transaction;
	using DatabaseType = Database;
	using ContextType = Context;
	using RandomType = Random;
	using StorageType = Storage;

	Workload(std::size_t coordinator_id, DatabaseType &db, RandomType &random, Partitioner &partitioner)
		: coordinator_id(coordinator_id)
		, db(db)
		, random(random)
                , partitioner(partitioner)
	{
	}

	static uint64_t next_transaction_id(uint64_t coordinator_id, uint64_t cluster_worker_id)
	{
		constexpr int coordinator_id_offset = 40;
		constexpr int worker_id_offset = 32;
		static std::atomic<int64_t> tid_static{ 1 };
		auto tid = tid_static.fetch_add(1);
		return (coordinator_id << coordinator_id_offset) | (cluster_worker_id << worker_id_offset) | tid;
	}

	std::unique_ptr<TransactionType> next_transaction(ContextType &context, std::size_t partition_id, std::size_t worker_id, std::size_t granule_id = 0)
	{
		int x = random.uniform_dist(1, 100);
                std::unique_ptr<TransactionType> p;

		static std::atomic<uint64_t> tid_cnt(0);
		long long transactionId = tid_cnt.fetch_add(1);
		auto random_seed = Time::now();

                std::string transactionType;
		random.set_seed(random_seed);
                if (context.workloadType == SmallBankWorkloadType::MIXED) {
                        if (x <= 15) {
                                p = std::make_unique<Balance<Transaction> >(coordinator_id, partition_id, granule_id, db, context, random, partitioner);
                        } else {
                                p = std::make_unique<Balance<Transaction> >(coordinator_id, partition_id, granule_id, db, context, random, partitioner);
                        }
                } else {
                        CHECK(0);
                }
		p->txn_random_seed_start = random_seed;
		p->transaction_id = next_transaction_id(coordinator_id, worker_id);
		return p;
	}

	std::unique_ptr<TransactionType> deserialize_from_raw(ContextType &context, const std::string &data)
	{
		CHECK(0);
	}

        // this function is co-designed with the TEST workload
        void perform_correctness_test(ContextType &context)
        {
        }

    private:
	std::size_t coordinator_id;
	DatabaseType &db;
	RandomType &random;
        Partitioner &partitioner;
};

} // namespace smallbank
} // namespace star
