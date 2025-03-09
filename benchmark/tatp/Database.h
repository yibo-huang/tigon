//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "benchmark/tatp/Context.h"
#include "benchmark/tatp/Random.h"
#include "benchmark/tatp/Schema.h"
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
#include <sstream>
#include <iomanip>

#include "common/CXLMemory.h"
#include "core/CXLTable.h"

namespace star
{
namespace tatp
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
                return 3;
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
                        // subscriber table
			auto subscriberTableID = tatp::subscriber::tableID;
			if (context.protocol == "Sundial") {
				tbl_subscriber_vec.push_back(
					std::make_unique<TableHashMap<997, tatp::subscriber::key, tatp::subscriber::value, tatp::subscriber::KeyComparator, tatp::subscriber::ValueComparator, MetaInitFuncSundial> >(subscriberTableID, partitionID));
                        } else if (context.protocol == "SundialPasha") {
                                tbl_subscriber_vec.push_back(
					std::make_unique<TableHashMap<997, tatp::subscriber::key, tatp::subscriber::value, tatp::subscriber::KeyComparator, tatp::subscriber::ValueComparator, MetaInitFuncSundialPasha> >(subscriberTableID, partitionID));
                        } else if (context.protocol == "TwoPL") {
                                tbl_subscriber_vec.push_back(
					std::make_unique<TableHashMap<997, tatp::subscriber::key, tatp::subscriber::value, tatp::subscriber::KeyComparator, tatp::subscriber::ValueComparator, MetaInitFuncTwoPL> >(subscriberTableID, partitionID));
                        } else if (context.protocol == "TwoPLPasha") {
                                tbl_subscriber_vec.push_back(
					std::make_unique<TableHashMap<997, tatp::subscriber::key, tatp::subscriber::value, tatp::subscriber::KeyComparator, tatp::subscriber::ValueComparator, MetaInitFuncTwoPLPasha> >(subscriberTableID, partitionID));
			} else if (context.protocol != "HStore") {
				CHECK(0);
			} else {
				CHECK(0);
			}

                        // secondary subscriber table
                        auto sec_subscriberTableID = tatp::sec_subscriber::tableID;
			if (context.protocol == "Sundial") {
				tbl_sec_subscriber_vec.push_back(
					std::make_unique<TableHashMap<997, tatp::sec_subscriber::key, tatp::sec_subscriber::value, tatp::sec_subscriber::KeyComparator, tatp::sec_subscriber::ValueComparator, MetaInitFuncSundial> >(sec_subscriberTableID, partitionID));
                        } else if (context.protocol == "SundialPasha") {
                                tbl_sec_subscriber_vec.push_back(
					std::make_unique<TableHashMap<997, tatp::sec_subscriber::key, tatp::sec_subscriber::value, tatp::sec_subscriber::KeyComparator, tatp::sec_subscriber::ValueComparator, MetaInitFuncSundialPasha> >(sec_subscriberTableID, partitionID));
                        } else if (context.protocol == "TwoPL") {
                                tbl_sec_subscriber_vec.push_back(
					std::make_unique<TableHashMap<997, tatp::sec_subscriber::key, tatp::sec_subscriber::value, tatp::sec_subscriber::KeyComparator, tatp::sec_subscriber::ValueComparator, MetaInitFuncTwoPL> >(sec_subscriberTableID, partitionID));
                        } else if (context.protocol == "TwoPLPasha") {
                                tbl_sec_subscriber_vec.push_back(
					std::make_unique<TableHashMap<997, tatp::sec_subscriber::key, tatp::sec_subscriber::value, tatp::sec_subscriber::KeyComparator, tatp::sec_subscriber::ValueComparator, MetaInitFuncTwoPLPasha> >(sec_subscriberTableID, partitionID));
			} else if (context.protocol != "HStore") {
				CHECK(0);
			} else {
				CHECK(0);
			}

                        // access_info table
                        auto accessInfoTableID = tatp::access_info::tableID;
			if (context.protocol == "Sundial") {
				tbl_access_info_vec.push_back(
					std::make_unique<TableHashMap<997, tatp::access_info::key, tatp::access_info::value, tatp::access_info::KeyComparator, tatp::access_info::ValueComparator, MetaInitFuncSundial> >(accessInfoTableID, partitionID));
                        } else if (context.protocol == "SundialPasha") {
                                tbl_access_info_vec.push_back(
					std::make_unique<TableHashMap<997, tatp::access_info::key, tatp::access_info::value, tatp::access_info::KeyComparator, tatp::access_info::ValueComparator, MetaInitFuncSundialPasha> >(accessInfoTableID, partitionID));
                        } else if (context.protocol == "TwoPL") {
                                tbl_access_info_vec.push_back(
					std::make_unique<TableHashMap<997, tatp::access_info::key, tatp::access_info::value, tatp::access_info::KeyComparator, tatp::access_info::ValueComparator, MetaInitFuncTwoPL> >(accessInfoTableID, partitionID));
                        } else if (context.protocol == "TwoPLPasha") {
                                tbl_access_info_vec.push_back(
					std::make_unique<TableHashMap<997, tatp::access_info::key, tatp::access_info::value, tatp::access_info::KeyComparator, tatp::access_info::ValueComparator, MetaInitFuncTwoPLPasha> >(accessInfoTableID, partitionID));
			} else if (context.protocol != "HStore") {
				CHECK(0);
			} else {
				CHECK(0);
			}

                        // special_facility table

                        // call_forwarding table
		}

		// there are 5 tables in TATP
		tbl_vecs.resize(get_table_num_per_partition());

		auto tFunc = [](std::unique_ptr<ITable> &table) { return table.get(); };

		std::transform(tbl_subscriber_vec.begin(), tbl_subscriber_vec.end(), std::back_inserter(tbl_vecs[0]), tFunc);
                std::transform(tbl_sec_subscriber_vec.begin(), tbl_sec_subscriber_vec.end(), std::back_inserter(tbl_vecs[1]), tFunc);
                std::transform(tbl_access_info_vec.begin(), tbl_access_info_vec.end(), std::back_inserter(tbl_vecs[2]), tFunc);

		using std::placeholders::_1;
		initTables("subscriber", [&context, this](std::size_t partitionID) { subscriberInit(context, partitionID); }, partitionNum, threadsNum, partitioner.get());
                initTables("sec_subscriber", [&context, this](std::size_t partitionID) { secSubscriberInit(context, partitionID); }, partitionNum, threadsNum, partitioner.get());
                initTables("access_info", [&context, this](std::size_t partitionID) { accessInfoInit(context, partitionID); }, partitionNum, threadsNum, partitioner.get());
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

                        // subscriber
                        auto subscriberTableID = subscriber::tableID;
                        cxl_tbl_vecs[subscriberTableID].resize(partitionNum);
                        auto subscriber_cxl_hashtables = reinterpret_cast<CCHashTable *>(cxl_memory.cxlalloc_malloc_wrapper(
                                        sizeof(CCHashTable) * partitionNum, CXLMemory::INDEX_ALLOCATION));
                        for (int i = 0; i < partitionNum; i++) {
                                auto cxl_table = &subscriber_cxl_hashtables[i];
                                new(cxl_table) CCHashTable(cxl_hashtable_bkt_cnt);
                                cxl_table_ptrs[subscriberTableID * partitionNum + i] = reinterpret_cast<void *>(cxl_table);
                                cxl_tbl_vecs[subscriberTableID][i] = new CXLTableHashMap<subscriber::key>(cxl_table, subscriberTableID, i);
                        }

                        // secondary subscriber
                        auto secSubscriberTableID = sec_subscriber::tableID;
                        cxl_tbl_vecs[secSubscriberTableID].resize(partitionNum);
                        auto sec_subscriber_cxl_hashtables = reinterpret_cast<CCHashTable *>(cxl_memory.cxlalloc_malloc_wrapper(
                                        sizeof(CCHashTable) * partitionNum, CXLMemory::INDEX_ALLOCATION));
                        for (int i = 0; i < partitionNum; i++) {
                                auto cxl_table = &sec_subscriber_cxl_hashtables[i];
                                new(cxl_table) CCHashTable(cxl_hashtable_bkt_cnt);
                                cxl_table_ptrs[secSubscriberTableID * partitionNum + i] = reinterpret_cast<void *>(cxl_table);
                                cxl_tbl_vecs[secSubscriberTableID][i] = new CXLTableHashMap<sec_subscriber::key>(cxl_table, secSubscriberTableID, i);
                        }

                        // access_info
                        auto accessInfoTableID = access_info::tableID;
                        cxl_tbl_vecs[accessInfoTableID].resize(partitionNum);
                        auto access_info_cxl_hashtables = reinterpret_cast<CCHashTable *>(cxl_memory.cxlalloc_malloc_wrapper(
                                        sizeof(CCHashTable) * partitionNum, CXLMemory::INDEX_ALLOCATION));
                        for (int i = 0; i < partitionNum; i++) {
                                auto cxl_table = &access_info_cxl_hashtables[i];
                                new(cxl_table) CCHashTable(cxl_hashtable_bkt_cnt);
                                cxl_table_ptrs[accessInfoTableID * partitionNum + i] = reinterpret_cast<void *>(cxl_table);
                                cxl_tbl_vecs[accessInfoTableID][i] = new CXLTableHashMap<access_info::key>(cxl_table, accessInfoTableID, i);
                        }

                        CXLMemory::commit_shared_data_initialization(CXLMemory::cxl_data_migration_root_index, cxl_table_ptrs);
                        LOG(INFO) << "TATP initializes data migration metadata";
                } else {
                        // other hosts wait and retrieve the CXL tables
                        void *tmp = NULL;
                        CXLMemory::wait_and_retrieve_cxl_shared_data(CXLMemory::cxl_data_migration_root_index, &tmp);
                        boost::interprocess::offset_ptr<void> *cxl_table_ptrs = reinterpret_cast<boost::interprocess::offset_ptr<void> *>(tmp);

                        // subscriber
                        auto subscriberTableID = subscriber::tableID;
                        cxl_tbl_vecs[subscriberTableID].resize(partitionNum);
                        for (int i = 0; i < partitionNum; i++) {
                                auto cxl_table = reinterpret_cast<CCHashTable *>(cxl_table_ptrs[subscriberTableID * partitionNum + i].get());
                                cxl_tbl_vecs[subscriberTableID][i] = new CXLTableHashMap<subscriber::key>(cxl_table, subscriberTableID, i);
                        }

                        // secondary subscriber
                        auto secSubscriberTableID = sec_subscriber::tableID;
                        cxl_tbl_vecs[secSubscriberTableID].resize(partitionNum);
                        for (int i = 0; i < partitionNum; i++) {
                                auto cxl_table = reinterpret_cast<CCHashTable *>(cxl_table_ptrs[secSubscriberTableID * partitionNum + i].get());
                                cxl_tbl_vecs[secSubscriberTableID][i] = new CXLTableHashMap<sec_subscriber::key>(cxl_table, secSubscriberTableID, i);
                        }

                        // access_info
                        auto accessInfoTableID = access_info::tableID;
                        cxl_tbl_vecs[accessInfoTableID].resize(partitionNum);
                        for (int i = 0; i < partitionNum; i++) {
                                auto cxl_table = reinterpret_cast<CCHashTable *>(cxl_table_ptrs[accessInfoTableID * partitionNum + i].get());
                                cxl_tbl_vecs[accessInfoTableID][i] = new CXLTableHashMap<access_info::key>(cxl_table, accessInfoTableID, i);
                        }
                        LOG(INFO) << "TATP retrieves data migration metadata";
                }

                return cxl_tbl_vecs;
        }

        void move_all_tables_into_cxl(std::function<migration_result(ITable *, const void *, std::tuple<MetaDataType *, void *> &, bool)> move_in_func)
        {
                for (int i = 0; i < tbl_vecs.size(); i++) {
                        for (int j = 0; j < tbl_vecs[i].size(); j++) {
                                tbl_vecs[i][j]->move_all_into_cxl(move_in_func);
                        }
                }
        }

        void move_non_part_tables_into_cxl(std::function<migration_result(ITable *, const void *, std::tuple<MetaDataType *, void *> &, bool)> move_in_func)
        {
                for (int i = 0; i < tbl_vecs.size(); i++) {
                        for (int j = 0; j < tbl_vecs[i].size(); j++) {
                                tbl_vecs[i][j]->move_all_into_cxl(move_in_func);
                        }
                }
        }

    private:
	void subscriberInit(const Context &context, std::size_t partitionID)
	{
		Random random;
		ITable *table = tbl_subscriber_vec[partitionID].get();

		std::size_t numSubScriberPerPartition = context.numSubScriberPerPartition;
		std::size_t partitionNum = context.partition_num;

                for (auto i = partitionID * numSubScriberPerPartition; i < (partitionID + 1) * numSubScriberPerPartition; i++) {
                        DCHECK(context.getPartitionID(i) == partitionID);

                        subscriber::key key(i);
                        subscriber::value value;

                        std::stringstream sub_nbr;
                        sub_nbr << std::setw(15) << std::setfill('0') << i;
                        value.SUB_NBR.assign(sub_nbr.str());

                        value.BITS.assign(random.a_string(SUBSCRIBER_BITS_SIZE, SUBSCRIBER_BITS_SIZE));
                        value.HEXES.assign(random.a_string(SUBSCRIBER_HEXES_SIZE, SUBSCRIBER_HEXES_SIZE));
                        value.BYTES.assign(random.a_string(SUBSCRIBER_BYTES_SIZE, SUBSCRIBER_BYTES_SIZE));
                        value.MSC_LOCATION = random.uniform_dist(0, UINT32_MAX);
                        value.VLR_LOCATION = random.uniform_dist(0, UINT32_MAX);

                        bool success = table->insert(&key, &value);
                        CHECK(success == true);
                }

                // insert a max key that represents the upper bound (for next-key locking)
                subscriber::key max_key(UINT32_MAX);
                subscriber::value dummy_value;
                bool success = table->insert(&max_key, &dummy_value);
                CHECK(success == true);
	}

        void secSubscriberInit(const Context &context, std::size_t partitionID)
	{
		Random random;
		ITable *table = tbl_sec_subscriber_vec[partitionID].get();

		std::size_t numSubScriberPerPartition = context.numSubScriberPerPartition;
		std::size_t partitionNum = context.partition_num;

                for (auto i = partitionID * numSubScriberPerPartition; i < (partitionID + 1) * numSubScriberPerPartition; i++) {
                        DCHECK(context.getPartitionID(i) == partitionID);

                        sec_subscriber::key key;

                        std::stringstream sub_nbr;
                        sub_nbr << std::setw(15) << std::setfill('0') << i;
                        key.SUB_NBR.assign(sub_nbr.str());

                        sec_subscriber::value value;
                        value.S_ID = i;

                        bool success = table->insert(&key, &value);
                        CHECK(success == true);
                }

                // secondary subscriber does not need a max key
                // // insert a max key that represents the upper bound (for next-key locking)
                // sec_subscriber::key max_key;

                // std::stringstream max_sub_nbr;
                // max_sub_nbr << std::setw(15) << std::setfill('0') << UINT32_MAX;
                // max_key.SUB_NBR.assign(max_sub_nbr.str());

                // sec_subscriber::value dummy_value;
                // bool success = table->insert(&max_key, &dummy_value);
                // CHECK(success == true);
	}

        void accessInfoInit(const Context &context, std::size_t partitionID)
	{
		Random random;
		ITable *table = tbl_access_info_vec[partitionID].get();

		std::size_t numSubScriberPerPartition = context.numSubScriberPerPartition;
		std::size_t partitionNum = context.partition_num;

                for (auto i = partitionID * numSubScriberPerPartition; i < (partitionID + 1) * numSubScriberPerPartition; i++) {
                        DCHECK(context.getPartitionID(i) == partitionID);

                        // uint64_t record_num = random.uniform_dist(1, 4);
                        uint64_t record_num = 4;        // FIXME: TATP may fail a search, but our system currently does not support failed search
                        std::vector<uint8_t> used_ai_types;
                        for (auto j = 0; j < record_num; j++) {
                                access_info::key key;
                                key.S_ID = i;

                                uint64_t unique_ai_type = 0;
                                while (true) {
                                        unique_ai_type = random.uniform_dist(1, 4);
                                        bool used = false;
                                        for (auto k = 0; k < used_ai_types.size(); k++) {
                                                if (unique_ai_type == used_ai_types[k]) {
                                                        used = true;
                                                }
                                        }
                                        if (used == false) {
                                                used_ai_types.push_back(unique_ai_type);
                                                key.AI_TYPE = unique_ai_type;
                                                break;
                                        }
                                }

                                access_info::value value;
                                value.DATA_1 = random.uniform_dist(0, 255);
                                value.DATA_2 = random.uniform_dist(0, 255);
                                value.DATA_3.assign(random.a_string(ACCESS_INFO_DATA_3_SIZE, ACCESS_INFO_DATA_3_SIZE));
                                value.DATA_4.assign(random.a_string(ACCESS_INFO_DATA_4_SIZE, ACCESS_INFO_DATA_4_SIZE));

                                bool success = table->insert(&key, &value);
                                CHECK(success == true);
                        }
                }

                // insert a max key that represents the upper bound (for next-key locking)
                access_info::key max_key(UINT32_MAX, UINT8_MAX);
                access_info::value dummy_value;
                bool success = table->insert(&max_key, &dummy_value);
                CHECK(success == true);
	}

    public:
        // for correctness test
        std::atomic<uint64_t> global_total_commit{ 0 };

    private:
        static constexpr uint64_t cxl_hashtable_bkt_cnt = 500000;

	std::vector<ThreadPool *> threadpools;
	WALLogger *checkpoint_file_writer = nullptr;

	std::vector<std::vector<ITable *> > tbl_vecs;
	std::vector<std::unique_ptr<ITable> > tbl_subscriber_vec;
	std::vector<std::unique_ptr<ITable> > tbl_sec_subscriber_vec;
        std::vector<std::unique_ptr<ITable> > tbl_access_info_vec;

        std::vector<std::vector<CXLTableBase *> > cxl_tbl_vecs;
};
} // namespace tatp
} // namespace star
