//
// Created by Yi Lu on 7/24/18.
//

#pragma once

#include <string>
#include "benchmark/tpcc/Context.h"
#include "benchmark/tpcc/Database.h"
#include "benchmark/tpcc/Random.h"
#include "benchmark/tpcc/Storage.h"
#include "benchmark/tpcc/Transaction.h"
#include "core/Partitioner.h"

namespace star
{

namespace tpcc
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
                , delivery_cur_sub_query_id(0)
	{
	}

	static uint64_t next_transaction_id(uint64_t coordinator_id)
	{
		constexpr int coordinator_id_offset = 32;
		static std::atomic<int64_t> tid_static{ 1 };
		auto tid = tid_static.fetch_add(1);
		return (coordinator_id << coordinator_id_offset) | tid;
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
		if (context.workloadType == TPCCWorkloadType::MIXED) {
                        if (x <= 4) {
                                p = std::make_unique<OrderStatus<Transaction> >(coordinator_id, partition_id, db, context, random, partitioner);
				transactionType = "TPCC OrderStatus";
                        } else if (x <= 4 + 4) {
                                p = std::make_unique<Delivery<Transaction> >(coordinator_id, partition_id, db, context, random, partitioner, delivery_cur_sub_query_id);
				transactionType = "TPCC Delivery";
                                delivery_cur_sub_query_id++;
                                if (delivery_cur_sub_query_id == DISTRICT_PER_WAREHOUSE)
                                        delivery_cur_sub_query_id = 0;
                        } else if (x <= 4 + 4 + 4) {
				p = std::make_unique<StockLevel<Transaction> >(coordinator_id, partition_id, db, context, random, partitioner);
				transactionType = "TPCC StockLevel";
                        } else if (x <= 4 + 4 + 4 + 43) {
				p = std::make_unique<Payment<Transaction> >(coordinator_id, partition_id, db, context, random, partitioner);
				transactionType = "TPCC Payment";
			} else {
				p = std::make_unique<NewOrder<Transaction> >(coordinator_id, partition_id, db, context, random, partitioner);
				transactionType = "TPCC NewOrder";
			}
                } else if (context.workloadType == TPCCWorkloadType::FIRST_TWO) {
                        if (x <= 50) {
                                p = std::make_unique<Payment<Transaction> >(coordinator_id, partition_id, db, context, random, partitioner);
				transactionType = "TPCC Payment";
			} else {
				p = std::make_unique<NewOrder<Transaction> >(coordinator_id, partition_id, db, context, random, partitioner);
				transactionType = "TPCC NewOrder";
			}
                } else if (context.workloadType == TPCCWorkloadType::TEST) {
			p = std::make_unique<Test<Transaction> >(coordinator_id, partition_id, db, context, random, partitioner);
			transactionType = "TPCC Test";
		} else if (context.workloadType == TPCCWorkloadType::NEW_ORDER_ONLY) {
			p = std::make_unique<NewOrder<Transaction> >(coordinator_id, partition_id, db, context, random, partitioner);
			transactionType = "TPCC NewOrder";
		} else {
			p = std::make_unique<Payment<Transaction> >(coordinator_id, partition_id, db, context, random, partitioner);
			transactionType = "TPCC Payment";
		}
		p->txn_random_seed_start = random_seed;
		p->transaction_id = next_transaction_id(coordinator_id);
		return p;
	}

	std::unique_ptr<TransactionType> deserialize_from_raw(ContextType &context, const std::string &data)
	{
		Decoder decoder(data);
		uint64_t seed;
		uint32_t txn_type;
		std::size_t ith_replica;
		std::size_t partition_id;
		int64_t transaction_id;
		uint64_t straggler_wait_time;
		decoder >> transaction_id >> txn_type >> straggler_wait_time >> ith_replica >> seed >> partition_id;
		RandomType random;
		random.set_seed(seed);

		if (txn_type == 0) {
			auto p = std::make_unique<NewOrder<Transaction> >(coordinator_id, partition_id, db, context, random, partitioner, ith_replica);
			p->txn_random_seed_start = seed;
			p->transaction_id = transaction_id;
			p->straggler_wait_time = straggler_wait_time;
			p->deserialize_lock_status(decoder);
			return p;
		} else {
			auto p = std::make_unique<Payment<Transaction> >(coordinator_id, partition_id, db, context, random, partitioner, ith_replica);
			p->txn_random_seed_start = seed;
			p->transaction_id = transaction_id;
			p->straggler_wait_time = straggler_wait_time;
			p->deserialize_lock_status(decoder);
			return p;
		}
	}

        // this function is co-designed with the TEST workload
        void perform_correctness_test(ContextType &context)
        {
                if (context.workloadType == TPCCWorkloadType::TEST) {
                        uint64_t total_c_payment_cnt = 0;
                        for (int i = 0; i < context.partition_num; i++) {
                                int32_t W_ID = i + 1;
                                int32_t D_ID = 1;
                                int32_t C_ID = 1;

                                // The row in the CUSTOMER table with matching C_W_ID, C_D_ID, and C_ID is selected

                                auto customerTableID = customer::tableID;
                                auto table = db.find_table(customerTableID, W_ID - 1);

                                auto customer_key = customer::key(W_ID, D_ID, C_ID);
                                customer::value &customer_value = *static_cast<customer::value *>(table->search_value(&customer_key));
                                LOG(INFO) << "WH = " << W_ID << "C_PAYMENT_CNT = " << customer_value.C_PAYMENT_CNT;
                                total_c_payment_cnt += customer_value.C_PAYMENT_CNT;
                        }

                        LOG(INFO) << "total_c_payment_cnt = " << total_c_payment_cnt << " , global_total_commit = " << db.global_total_commit;
                        CHECK(total_c_payment_cnt - context.partition_num == db.global_total_commit);
                }
        }

    private:
	std::size_t coordinator_id;
	DatabaseType &db;
	RandomType &random;
	Partitioner &partitioner;

        uint64_t delivery_cur_sub_query_id;
};

} // namespace tpcc
} // namespace star
