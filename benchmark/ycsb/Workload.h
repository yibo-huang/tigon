//
// Created by Yi Lu on 7/25/18.
//

#pragma once

#include "benchmark/tpcc/Context.h"
#include "benchmark/ycsb/Database.h"
#include "benchmark/ycsb/Random.h"
#include "benchmark/ycsb/Storage.h"
#include "benchmark/ycsb/Transaction.h"
#include "core/Partitioner.h"

namespace star
{

namespace ycsb
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
                if (context.workloadType == YCSBWorkloadType::MIXED) {
                        if (x <= 80) {
                                p = std::make_unique<Scan<Transaction> >(coordinator_id, partition_id, granule_id, db, context, random, partitioner);
                        } else if (x <= 90) {
                                p = std::make_unique<Insert<Transaction> >(coordinator_id, partition_id, granule_id, db, context, random, partitioner);
                        } else {
                                p = std::make_unique<Delete<Transaction> >(coordinator_id, partition_id, granule_id, db, context, random, partitioner);
                        }
                } else if (context.workloadType == YCSBWorkloadType::RMW) {
                        p = std::make_unique<ReadModifyWrite<Transaction> >(coordinator_id, partition_id, granule_id, db, context, random, partitioner);
                } else if (context.workloadType == YCSBWorkloadType::SCAN) {
                        p = std::make_unique<Scan<Transaction> >(coordinator_id, partition_id, granule_id, db, context, random, partitioner);
                } else if (context.workloadType == YCSBWorkloadType::INSERT) {
                        p = std::make_unique<Insert<Transaction> >(coordinator_id, partition_id, granule_id, db, context, random, partitioner);
                } else if (context.workloadType == YCSBWorkloadType::DELETE) {
                        p = std::make_unique<Delete<Transaction> >(coordinator_id, partition_id, granule_id, db, context, random, partitioner);
                } else {
                        CHECK(0);
                }
		p->txn_random_seed_start = random_seed;
		p->transaction_id = next_transaction_id(coordinator_id, worker_id);
		return p;
	}

	std::unique_ptr<TransactionType> deserialize_from_raw(ContextType &context, const std::string &data)
	{
		Decoder decoder(data);
		uint64_t seed;
		std::size_t ith_replica;
		std::size_t partition_id;
		std::size_t granule_id;
		int32_t partition_count;
		int64_t transaction_id;
		uint64_t straggler_wait_time;

		// std::vector<int32_t> partitions_from_command, granules_from_command;
		// int32_t granule_count = 0;
		decoder >> transaction_id >> straggler_wait_time >> ith_replica >> seed >> partition_id >> granule_id >> partition_count;
		// for (int32_t i = 0; i < partition_count; ++i){
		//   int32_t p;
		//   decoder >> p;
		//   partitions_from_command.push_back(p);
		// }
		// decoder >> granule_count;
		// for (int32_t i = 0; i < granule_count; ++i){
		//   int32_t g;
		//   decoder >> g;
		//   granules_from_command.push_back(g);
		// }
		RandomType random;
		random.set_seed(seed);

		std::unique_ptr<TransactionType> p = std::make_unique<ReadModifyWrite<Transaction> >(coordinator_id, partition_id, granule_id, db, context,
												     random, partitioner, ith_replica);
		p->txn_random_seed_start = seed;
		DCHECK(p->get_partition_count() == partition_count);
		// std::vector<int32_t> partitions, granules;
		// for (int32_t i = 0; i < partition_count; ++i){
		//   partitions.push_back(p->get_partition(i));
		//   for (int32_t j = 0; j < p->get_partition_granule_count(i); ++j) {
		//     granules.push_back(p->get_granule(i, j));
		//   }
		// }
		// sort(granules.begin(), granules.end());
		// sort(partitions.begin(), partitions.end());
		// sort(partitions_from_command.begin(), partitions_from_command.end());
		// sort(granules_from_command.begin(), granules_from_command.end());
		// DCHECK(granules == granules_from_command);
		// DCHECK(partitions == partitions_from_command);
		p->transaction_id = transaction_id;
		p->straggler_wait_time = straggler_wait_time;
		p->deserialize_lock_status(decoder);
		return p;
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

} // namespace ycsb
} // namespace star
