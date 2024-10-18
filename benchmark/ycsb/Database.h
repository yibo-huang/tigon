//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "benchmark/ycsb/Context.h"
#include "benchmark/ycsb/Random.h"
#include "benchmark/ycsb/Schema.h"
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
namespace ycsb
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
                return 1;
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
			auto ycsbTableID = ycsb::tableID;
			if (context.protocol == "Sundial") {
				tbl_ycsb_vec.push_back(
					std::make_unique<TableBTreeOLC<ycsb::key, ycsb::value, ycsb::KeyComparator, ycsb::ValueComparator, MetaInitFuncSundial> >(ycsbTableID, partitionID));
                        } else if (context.protocol == "SundialPasha") {
                                tbl_ycsb_vec.push_back(
					std::make_unique<TableBTreeOLC<ycsb::key, ycsb::value, ycsb::KeyComparator, ycsb::ValueComparator, MetaInitFuncSundialPasha> >(ycsbTableID, partitionID));
                        } else if (context.protocol == "TwoPL") {
                                tbl_ycsb_vec.push_back(
					std::make_unique<TableBTreeOLC<ycsb::key, ycsb::value, ycsb::KeyComparator, ycsb::ValueComparator, MetaInitFuncTwoPL> >(ycsbTableID, partitionID));
                        } else if (context.protocol == "TwoPLPasha") {
                                tbl_ycsb_vec.push_back(
					std::make_unique<TableBTreeOLC<ycsb::key, ycsb::value, ycsb::KeyComparator, ycsb::ValueComparator, MetaInitFuncTwoPLPasha> >(ycsbTableID, partitionID));
			} else if (context.protocol != "HStore") {
				tbl_ycsb_vec.push_back(std::make_unique<TableHashMap<997, ycsb::key, ycsb::value> >(ycsbTableID, partitionID));
			} else {
				if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON ||
				    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF ||
				    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
					tbl_ycsb_vec.push_back(std::make_unique<HStoreCOWTable<997, ycsb::key, ycsb::value> >(ycsbTableID, partitionID));
				} else {
					tbl_ycsb_vec.push_back(std::make_unique<HStoreTable<ycsb::key, ycsb::value> >(ycsbTableID, partitionID));
				}
			}
		}

		// there is 1 table in ycsb
		tbl_vecs.resize(1);

		auto tFunc = [](std::unique_ptr<ITable> &table) { return table.get(); };

		std::transform(tbl_ycsb_vec.begin(), tbl_ycsb_vec.end(), std::back_inserter(tbl_vecs[0]), tFunc);

		using std::placeholders::_1;
		initTables(
			"ycsb", [&context, this](std::size_t partitionID) { ycsbInit(context, partitionID); }, partitionNum, threadsNum, partitioner.get());
	}

	void apply_operation(const Operation &operation)
	{
		CHECK(false); // not supported
	}

	void start_checkpoint_process(const std::vector<int> &partitions)
	{
		static thread_local std::vector<char> checkpoint_buffer;
		checkpoint_buffer.reserve(8 * 1024 * 1024);
		const std::size_t write_buffer_threshold = 128 * 1024;
		for (auto partitionID : partitions) {
			ITable *table = find_table(0, partitionID);
			table->turn_on_cow();
			threadpools[partitionID % 6]->enqueue([this, write_buffer_threshold, table]() {
				table->dump_copy(
					[&, this, table](const void *k, const void *v) {
						std::size_t size_needed = table->key_size();
						auto write_idx = checkpoint_buffer.size();
						checkpoint_buffer.resize(size_needed + checkpoint_buffer.size());
						memcpy(&checkpoint_buffer[write_idx], (const char *)k, table->key_size());

						size_needed = table->value_size();
						write_idx = checkpoint_buffer.size();
						checkpoint_buffer.resize(size_needed + checkpoint_buffer.size());
						memcpy(&checkpoint_buffer[write_idx], (const char *)v, table->value_size());
					},
					[&, this, write_buffer_threshold, table]() { // Called when the table is unlocked
						if (checkpoint_buffer.size() >= write_buffer_threshold) {
							this->checkpoint_file_writer->write(&checkpoint_buffer[0], checkpoint_buffer.size(), false);
							checkpoint_buffer.clear();
						}
						// checkpoint_buffer.clear();
					});
			});
		}
	}

	bool checkpoint_work_finished(const std::vector<int> &partitions)
	{
		for (auto partitionID : partitions) {
			ITable *table = find_table(0, partitionID);
			if (table->cow_dump_finished() == false)
				return false;
		}
		return true;
	}

	void stop_checkpoint_process(const std::vector<int> &partitions)
	{
		for (auto partitionID : partitions) {
			ITable *table = find_table(0, partitionID);
			auto cleanup_work = table->turn_off_cow();
			threadpools[partitionID % 6]->enqueue(cleanup_work);
		}
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

                        auto ycsbTableID = ycsb::tableID;
                        cxl_tbl_vecs[ycsbTableID].resize(partitionNum);
                        auto ycsb_cxl_btreetables = reinterpret_cast<CXLTableBTreeOLC<ycsb::key, ycsb::KeyComparator>::CXLBTree *>(cxl_memory.cxlalloc_malloc_wrapper(
                                        sizeof(CXLTableBTreeOLC<ycsb::key, ycsb::KeyComparator>::CXLBTree) * partitionNum, CXLMemory::INDEX_ALLOCATION));
                        for (int i = 0; i < partitionNum; i++) {
                                auto cxl_table = &ycsb_cxl_btreetables[i];
                                new(cxl_table) CXLTableBTreeOLC<ycsb::key, ycsb::KeyComparator>::CXLBTree();
                                cxl_table_ptrs[ycsbTableID * partitionNum + i] = reinterpret_cast<void *>(cxl_table);
                                cxl_tbl_vecs[ycsbTableID][i] = new CXLTableBTreeOLC<ycsb::key, ycsb::KeyComparator>(cxl_table, ycsbTableID, i);
                        }

                        CXLMemory::commit_shared_data_initialization(CXLMemory::cxl_data_migration_root_index, cxl_table_ptrs);
                        LOG(INFO) << "YCSB initializes data migration metadata";
                } else {
                        // other hosts wait and retrieve the CXL tables
                        void *tmp = NULL;
                        CXLMemory::wait_and_retrieve_cxl_shared_data(CXLMemory::cxl_data_migration_root_index, &tmp);
                        boost::interprocess::offset_ptr<void> *cxl_table_ptrs = reinterpret_cast<boost::interprocess::offset_ptr<void> *>(tmp);

                        auto ycsbTableID = ycsb::tableID;
                        cxl_tbl_vecs[ycsbTableID].resize(partitionNum);
                        for (int i = 0; i < partitionNum; i++) {
                                auto cxl_table = reinterpret_cast<CXLTableBTreeOLC<ycsb::key, ycsb::KeyComparator>::CXLBTree *>(cxl_table_ptrs[ycsbTableID * partitionNum + i].get());
                                cxl_tbl_vecs[ycsbTableID][i] = new CXLTableBTreeOLC<ycsb::key, ycsb::KeyComparator>(cxl_table, ycsbTableID, i);
                        }
                        LOG(INFO) << "YCSB retrieves data migration metadata";
                }

                return cxl_tbl_vecs;
        }

        void move_all_tables_into_cxl(std::function<bool(ITable *, const void *, std::tuple<MetaDataType *, void *> &)> move_in_func)
        {
                for (int i = 0; i < tbl_vecs.size(); i++) {
                        for (int j = 0; j < tbl_vecs[i].size(); j++) {
                                tbl_vecs[i][j]->move_all_into_cxl(move_in_func);
                        }
                }
        }

        void move_non_part_tables_into_cxl(std::function<bool(ITable *, const void *, std::tuple<MetaDataType *, void *> &)> move_in_func)
        {
                for (int i = 0; i < tbl_vecs.size(); i++) {
                        for (int j = 0; j < tbl_vecs[i].size(); j++) {
                                tbl_vecs[i][j]->move_all_into_cxl(move_in_func);
                        }
                }
        }

    private:
	void ycsbInit(const Context &context, std::size_t partitionID)
	{
		Random random;
		ITable *table = tbl_ycsb_vec[partitionID].get();

		std::size_t keysPerPartition = context.keysPerPartition; // 5M keys per partition
		std::size_t partitionNum = context.partition_num;
		std::size_t totalKeys = keysPerPartition * partitionNum;

		if (context.strategy == PartitionStrategy::RANGE) {
			// use range partitioning

			for (auto i = partitionID * keysPerPartition; i < (partitionID + 1) * keysPerPartition; i++) {
				DCHECK(context.getPartitionID(i) == partitionID);

				ycsb::key key(i);
				ycsb::value value;
				value.Y_F01.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
				value.Y_F02.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
				value.Y_F03.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
				value.Y_F04.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
				value.Y_F05.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
				value.Y_F06.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
				value.Y_F07.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
				value.Y_F08.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
				value.Y_F09.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
				value.Y_F10.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));

				bool success = table->insert(&key, &value);
                                CHECK(success == true);
			}

		} else {
			// use round-robin hash partitioning

			for (auto i = partitionID; i < totalKeys; i += partitionNum) {
				DCHECK(context.getPartitionID(i) == partitionID);

				ycsb::key key(i);
				ycsb::value value;
				value.Y_F01.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
				value.Y_F02.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
				value.Y_F03.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
				value.Y_F04.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
				value.Y_F05.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
				value.Y_F06.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
				value.Y_F07.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
				value.Y_F08.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
				value.Y_F09.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));
				value.Y_F10.assign(random.a_string(YCSB_FIELD_SIZE, YCSB_FIELD_SIZE));

				bool success = table->insert(&key, &value);
                                CHECK(success == true);
			}
		}
	}

    public:
        // for correctness test
        std::atomic<uint64_t> global_total_commit{ 0 };

    private:
        static constexpr uint64_t cxl_hashtable_bkt_cnt = 50000;

	std::vector<ThreadPool *> threadpools;
	WALLogger *checkpoint_file_writer = nullptr;

	std::vector<std::vector<ITable *> > tbl_vecs;
	std::vector<std::unique_ptr<ITable> > tbl_ycsb_vec;

        std::vector<std::vector<CXLTableBase *> > cxl_tbl_vecs;
};
} // namespace ycsb
} // namespace star
