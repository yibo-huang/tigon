//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "benchmark/smallbank/Context.h"
#include "benchmark/smallbank/Random.h"
#include "benchmark/smallbank/Schema.h"
#include "common/Operation.h"
#include "common/ThreadPool.h"
#include "core/Macros.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <glog/logging.h>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/CXLMemory.h"
#include "core/CXLTable.h"

namespace star
{
namespace smallbank
{
class Database {
    public:
	using MetaDataType = std::atomic<uint64_t>;
	using ContextType = Context;
	using RandomType = Random;

        std::size_t get_total_table_num()
        {
                std::size_t table_num = 0;
                for (int i = 0; i < tbl_vecs.size(); i++)
                        table_num += tbl_vecs[i].size();
                return table_num;
        }

        std::size_t get_table_num_per_partition()
        {
                return 2;
        }

	ITable *find_table(std::size_t table_id, std::size_t partition_id)
	{
		DCHECK(table_id < tbl_vecs.size());
		DCHECK(partition_id < tbl_vecs[table_id].size());
		return tbl_vecs[table_id][partition_id];
	}

	template <class InitFunc>
	void initTables(const std::string &name, InitFunc initFunc, std::size_t partitionNum, std::size_t threadsNum, Partitioner *partitioner)
	{
		std::vector<int> all_parts;

		for (auto i = 0u; i < partitionNum; i++) {
			if (partitioner == nullptr || partitioner->is_partition_replicated_on_me(i)) {
				all_parts.push_back(i);
			}
		}

		std::vector<std::thread> v;
		auto now = std::chrono::steady_clock::now();

		for (auto threadID = 0u; threadID < threadsNum; threadID++) {
			v.emplace_back([=]() {
				for (auto i = threadID; i < all_parts.size(); i += threadsNum) {
					auto partitionID = all_parts[i];
					initFunc(partitionID);
				}
			});
		}
		for (auto &t : v) {
			t.join();
		}
		LOG(INFO) << name << " initialization finished in "
			  << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - now).count() << " milliseconds.";
	}

	void initialize(const Context &context)
	{
		if (context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
			for (int i = 0; i < 6; ++i) {
				threadpools.push_back(new ThreadPool(1));
			}
			checkpoint_file_writer = new SimpleWALLogger(context.lotus_checkpoint_location);
		}
		std::size_t coordinator_id = context.coordinator_id;
		std::size_t partitionNum = context.partition_num;
		std::size_t threadsNum = context.worker_num;

		auto partitioner = PartitionerFactory::create_partitioner(context.partitioner, coordinator_id, context.coordinator_num);

		for (auto partitionID = 0u; partitionID < partitionNum; partitionID++) {
                        // savings table
			auto savingsTableID = smallbank::savings::tableID;
			if (context.protocol == "Sundial") {
				tbl_savings_vec.push_back(
					std::make_unique<TableBTreeOLC<smallbank::savings::key, smallbank::savings::value, smallbank::savings::KeyComparator, smallbank::savings::ValueComparator, MetaInitFuncSundial> >(savingsTableID, partitionID));
                        } else if (context.protocol == "SundialPasha") {
                                tbl_savings_vec.push_back(
					std::make_unique<TableBTreeOLC<smallbank::savings::key, smallbank::savings::value, smallbank::savings::KeyComparator, smallbank::savings::ValueComparator, MetaInitFuncSundialPasha> >(savingsTableID, partitionID));
                        } else if (context.protocol == "TwoPL") {
                                tbl_savings_vec.push_back(
					std::make_unique<TableBTreeOLC<smallbank::savings::key, smallbank::savings::value, smallbank::savings::KeyComparator, smallbank::savings::ValueComparator, MetaInitFuncTwoPL> >(savingsTableID, partitionID));
                        } else if (context.protocol == "TwoPLPasha") {
                                tbl_savings_vec.push_back(
					std::make_unique<TableBTreeOLC<smallbank::savings::key, smallbank::savings::value, smallbank::savings::KeyComparator, smallbank::savings::ValueComparator, MetaInitFuncTwoPLPasha> >(savingsTableID, partitionID));
			} else if (context.protocol != "HStore") {
				CHECK(0);
			} else {
				CHECK(0);
			}

                        // checking table
			auto checkingTableID = smallbank::checking::tableID;
			if (context.protocol == "Sundial") {
				tbl_checking_vec.push_back(
					std::make_unique<TableBTreeOLC<smallbank::checking::key, smallbank::checking::value, smallbank::checking::KeyComparator, smallbank::checking::ValueComparator, MetaInitFuncSundial> >(checkingTableID, partitionID));
                        } else if (context.protocol == "SundialPasha") {
                                tbl_checking_vec.push_back(
					std::make_unique<TableBTreeOLC<smallbank::checking::key, smallbank::checking::value, smallbank::checking::KeyComparator, smallbank::checking::ValueComparator, MetaInitFuncSundialPasha> >(checkingTableID, partitionID));
                        } else if (context.protocol == "TwoPL") {
                                tbl_checking_vec.push_back(
					std::make_unique<TableBTreeOLC<smallbank::checking::key, smallbank::checking::value, smallbank::checking::KeyComparator, smallbank::checking::ValueComparator, MetaInitFuncTwoPL> >(checkingTableID, partitionID));
                        } else if (context.protocol == "TwoPLPasha") {
                                tbl_checking_vec.push_back(
					std::make_unique<TableBTreeOLC<smallbank::checking::key, smallbank::checking::value, smallbank::checking::KeyComparator, smallbank::checking::ValueComparator, MetaInitFuncTwoPLPasha> >(checkingTableID, partitionID));
			} else if (context.protocol != "HStore") {
				CHECK(0);
			} else {
				CHECK(0);
			}
		}

		// there is 2 tables in smallbank
		tbl_vecs.resize(2);

		auto tFunc = [](std::unique_ptr<ITable> &table) { return table.get(); };

		std::transform(tbl_savings_vec.begin(), tbl_savings_vec.end(), std::back_inserter(tbl_vecs[0]), tFunc);
		std::transform(tbl_checking_vec.begin(), tbl_checking_vec.end(), std::back_inserter(tbl_vecs[1]), tFunc);

		using std::placeholders::_1;
		initTables("savings", [&context, this](std::size_t partitionID) { savingsInit(context, partitionID); }, partitionNum, threadsNum, partitioner.get());
		initTables("checking", [&context, this](std::size_t partitionID) { checkingInit(context, partitionID); }, partitionNum, threadsNum, partitioner.get());
	}

	void apply_operation(const Operation &operation)
	{
		CHECK(0);
	}

	void start_checkpoint_process(const std::vector<int> &partitions)
	{
		CHECK(0);
	}

	bool checkpoint_work_finished(const std::vector<int> &partitions)
	{
		CHECK(0);
	}

	void stop_checkpoint_process(const std::vector<int> &partitions)
	{
                CHECK(0);
	}

	~Database()
	{
		for (size_t i = 0; i < threadpools.size(); ++i) {
			delete threadpools[i];
		}
	}

        std::vector<std::vector<CXLTableBase *> > &create_or_retrieve_cxl_tables(const Context &context)
        {
                std::size_t coordinator_id = context.coordinator_id;
		std::size_t partitionNum = context.partition_num;
                std::size_t table_num_per_partition = get_table_num_per_partition();
                std::size_t total_table_num = table_num_per_partition * partitionNum;

                cxl_tbl_vecs.resize(table_num_per_partition);

                if (coordinator_id == 0) {
                        // host 0 is responsible for creating the CXL tables
                        boost::interprocess::offset_ptr<void> *cxl_table_ptrs = reinterpret_cast<boost::interprocess::offset_ptr<void> *>(cxl_memory.cxlalloc_malloc_wrapper(
                                        sizeof(boost::interprocess::offset_ptr<void>) * total_table_num, CXLMemory::INDEX_ALLOCATION));

                        auto savingsTableID = savings::tableID;
                        cxl_tbl_vecs[savingsTableID].resize(partitionNum);
                        auto savings_cxl_btreetables = reinterpret_cast<CXLTableBTreeOLC<savings::key, savings::KeyComparator>::CXLBTree *>(cxl_memory.cxlalloc_malloc_wrapper(
                                        sizeof(CXLTableBTreeOLC<savings::key, savings::KeyComparator>::CXLBTree) * partitionNum, CXLMemory::INDEX_ALLOCATION));
                        for (int i = 0; i < partitionNum; i++) {
                                auto cxl_table = &savings_cxl_btreetables[i];
                                new(cxl_table) CXLTableBTreeOLC<savings::key, savings::KeyComparator>::CXLBTree();
                                cxl_table_ptrs[savingsTableID * partitionNum + i] = reinterpret_cast<void *>(cxl_table);
                                cxl_tbl_vecs[savingsTableID][i] = new CXLTableBTreeOLC<savings::key, savings::KeyComparator>(cxl_table, savingsTableID, i);
                        }

                        auto checkingTableID = checking::tableID;
                        cxl_tbl_vecs[checkingTableID].resize(partitionNum);
                        auto checking_cxl_btreetables = reinterpret_cast<CXLTableBTreeOLC<checking::key, checking::KeyComparator>::CXLBTree *>(cxl_memory.cxlalloc_malloc_wrapper(
                                        sizeof(CXLTableBTreeOLC<checking::key, checking::KeyComparator>::CXLBTree) * partitionNum, CXLMemory::INDEX_ALLOCATION));
                        for (int i = 0; i < partitionNum; i++) {
                                auto cxl_table = &checking_cxl_btreetables[i];
                                new(cxl_table) CXLTableBTreeOLC<checking::key, checking::KeyComparator>::CXLBTree();
                                cxl_table_ptrs[checkingTableID * partitionNum + i] = reinterpret_cast<void *>(cxl_table);
                                cxl_tbl_vecs[checkingTableID][i] = new CXLTableBTreeOLC<checking::key, checking::KeyComparator>(cxl_table, checkingTableID, i);
                        }

                        CXLMemory::commit_shared_data_initialization(CXLMemory::cxl_data_migration_root_index, cxl_table_ptrs);
                        LOG(INFO) << "SmallBank initializes data migration metadata";
                } else {
                        // other hosts wait and retrieve the CXL tables
                        void *tmp = NULL;
                        CXLMemory::wait_and_retrieve_cxl_shared_data(CXLMemory::cxl_data_migration_root_index, &tmp);
                        boost::interprocess::offset_ptr<void> *cxl_table_ptrs = reinterpret_cast<boost::interprocess::offset_ptr<void> *>(tmp);

                        auto savingsTableID = savings::tableID;
                        cxl_tbl_vecs[savingsTableID].resize(partitionNum);
                        for (int i = 0; i < partitionNum; i++) {
                                auto cxl_table = reinterpret_cast<CXLTableBTreeOLC<savings::key, savings::KeyComparator>::CXLBTree *>(cxl_table_ptrs[savingsTableID * partitionNum + i].get());
                                cxl_tbl_vecs[savingsTableID][i] = new CXLTableBTreeOLC<savings::key, savings::KeyComparator>(cxl_table, savingsTableID, i);
                        }

                        auto checkingTableID = checking::tableID;
                        cxl_tbl_vecs[checkingTableID].resize(partitionNum);
                        for (int i = 0; i < partitionNum; i++) {
                                auto cxl_table = reinterpret_cast<CXLTableBTreeOLC<checking::key, checking::KeyComparator>::CXLBTree *>(cxl_table_ptrs[checkingTableID * partitionNum + i].get());
                                cxl_tbl_vecs[checkingTableID][i] = new CXLTableBTreeOLC<checking::key, checking::KeyComparator>(cxl_table, checkingTableID, i);
                        }
                        LOG(INFO) << "SmallBank retrieves data migration metadata";
                }

                return cxl_tbl_vecs;
        }

        void move_all_tables_into_cxl(std::function<bool(ITable *, const void *, std::tuple<MetaDataType *, void *> &, bool)> move_in_func)
        {
                for (int i = 0; i < tbl_vecs.size(); i++) {
                        for (int j = 0; j < tbl_vecs[i].size(); j++) {
                                tbl_vecs[i][j]->move_all_into_cxl(move_in_func);
                        }
                }
        }

        void move_non_part_tables_into_cxl(std::function<bool(ITable *, const void *, std::tuple<MetaDataType *, void *> &, bool)> move_in_func)
        {
                for (int i = 0; i < tbl_vecs.size(); i++) {
                        for (int j = 0; j < tbl_vecs[i].size(); j++) {
                                tbl_vecs[i][j]->move_all_into_cxl(move_in_func);
                        }
                }
        }

    private:
	void savingsInit(const Context &context, std::size_t partitionID)
	{
		Random random;
		ITable *table = tbl_savings_vec[partitionID].get();

		std::size_t accountsPerPartition = context.accountsPerPartition;
		std::size_t partitionNum = context.partition_num;
		std::size_t totalAccounts = accountsPerPartition * partitionNum;

                for (auto i = partitionID * accountsPerPartition; i < (partitionID + 1) * accountsPerPartition; i++) {
                        savings::key key(i);
                        savings::value value(1000000000ull);    // same as Motor

                        bool success = table->insert(&key, &value);
                        CHECK(success == true);
                }

                // insert a max key that represents the upper bound (for next-key locking)
                savings::key max_key(UINT64_MAX);
                savings::value dummy_value;
                bool success = table->insert(&max_key, &dummy_value);
                CHECK(success == true);
	}

        void checkingInit(const Context &context, std::size_t partitionID)
	{
		Random random;
		ITable *table = tbl_checking_vec[partitionID].get();

		std::size_t accountsPerPartition = context.accountsPerPartition;
		std::size_t partitionNum = context.partition_num;
		std::size_t totalAccounts = accountsPerPartition * partitionNum;

                for (auto i = partitionID * accountsPerPartition; i < (partitionID + 1) * accountsPerPartition; i++) {
                        checking::key key(i);
                        checking::value value(1000000000ull);    // same as Motor

                        bool success = table->insert(&key, &value);
                        CHECK(success == true);
                }

                // insert a max key that represents the upper bound (for next-key locking)
                checking::key max_key(UINT64_MAX);
                checking::value dummy_value;
                bool success = table->insert(&max_key, &dummy_value);
                CHECK(success == true);
	}

    public:
        // for correctness test
        std::atomic<uint64_t> global_total_commit{ 0 };

    private:
        static constexpr uint64_t cxl_hashtable_bkt_cnt = 50000;

	std::vector<ThreadPool *> threadpools;
	WALLogger *checkpoint_file_writer = nullptr;

	std::vector<std::vector<ITable *> > tbl_vecs;
	std::vector<std::unique_ptr<ITable> > tbl_savings_vec;
	std::vector<std::unique_ptr<ITable> > tbl_checking_vec;

        std::vector<std::vector<CXLTableBase *> > cxl_tbl_vecs;
};
} // namespace ycsb
} // namespace star
