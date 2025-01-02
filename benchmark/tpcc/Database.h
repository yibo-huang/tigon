//
// Created by Yi Lu on 7/18/18.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <random>
#include <thread>
#include <unordered_map>
#include <vector>

#include "benchmark/tpcc/Context.h"
#include "benchmark/tpcc/Random.h"
#include "benchmark/tpcc/Schema.h"
#include "common/Operation.h"
#include "common/ThreadPool.h"
#include "common/WALLogger.h"
#include "common/Time.h"
#include "core/Macros.h"
#include "core/Partitioner.h"
#include "core/Table.h"
#include <glog/logging.h>

#include "common/CXLMemory.h"
#include "core/CXLTable.h"

namespace star
{
namespace tpcc
{

class Database {
    public:
	using MetaDataType = std::atomic<uint64_t>;
	using ContextType = Context;
	using RandomType = Random;

        std::size_t get_total_table_num()
        {
                std::size_t table_num = 0;
                for (int i = 0; i < tbl_vecs.size(); i++) {
                        LOG(INFO) << "table " << i << " = " << tbl_vecs[i].size();
                        table_num += tbl_vecs[i].size();
                }
                return table_num;
        }

        std::size_t get_table_num_per_partition()
        {
                return 11;
        }

	ITable *find_table(std::size_t table_id, std::size_t partition_id)
	{
		DCHECK(table_id < tbl_vecs.size());
		DCHECK(partition_id < tbl_vecs[table_id].size());
		return tbl_vecs[table_id][partition_id];
	}

	ITable *tbl_warehouse(std::size_t partition_id)
	{
		DCHECK(partition_id < tbl_warehouse_vec.size());
		return tbl_warehouse_vec[partition_id].get();
	}

	ITable *tbl_district(std::size_t partition_id)
	{
		DCHECK(partition_id < tbl_district_vec.size());
		return tbl_district_vec[partition_id].get();
	}

	ITable *tbl_customer(std::size_t partition_id)
	{
		DCHECK(partition_id < tbl_customer_vec.size());
		return tbl_customer_vec[partition_id].get();
	}

	ITable *tbl_customer_name_idx(std::size_t partition_id)
	{
		DCHECK(partition_id < tbl_customer_name_idx_vec.size());
		return tbl_customer_name_idx_vec[partition_id].get();
	}

	ITable *tbl_history(std::size_t partition_id)
	{
		DCHECK(partition_id < tbl_history_vec.size());
		return tbl_history_vec[partition_id].get();
	}

	ITable *tbl_new_order(std::size_t partition_id)
	{
		DCHECK(partition_id < tbl_new_order_vec.size());
		return tbl_new_order_vec[partition_id].get();
	}

	ITable *tbl_order(std::size_t partition_id)
	{
		DCHECK(partition_id < tbl_order_vec.size());
		return tbl_order_vec[partition_id].get();
	}

	ITable *tbl_order_line(std::size_t partition_id)
	{
		DCHECK(partition_id < tbl_order_line_vec.size());
		return tbl_order_line_vec[partition_id].get();
	}

	ITable *tbl_item(std::size_t partition_id)
	{
		DCHECK(partition_id < tbl_item_vec.size());
		return tbl_item_vec[partition_id].get();
	}

	ITable *tbl_stock(std::size_t partition_id)
	{
		DCHECK(partition_id < tbl_stock_vec.size());
		return tbl_stock_vec[partition_id].get();
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

		auto now = std::chrono::steady_clock::now();

		LOG(INFO) << "creating hash tables for database...";

		for (auto partitionID = 0u; partitionID < partitionNum; partitionID++) {
			auto warehouseTableID = warehouse::tableID;
			if (context.protocol == "Sundial") {
				tbl_warehouse_vec.push_back(
					std::make_unique<TableBTreeOLC<warehouse::key, warehouse::value, warehouse::KeyComparator, warehouse::ValueComparator, MetaInitFuncSundial> >(warehouseTableID, partitionID));
                        } else if (context.protocol == "SundialPasha") {
				tbl_warehouse_vec.push_back(
					std::make_unique<TableBTreeOLC<warehouse::key, warehouse::value, warehouse::KeyComparator, warehouse::ValueComparator, MetaInitFuncSundialPasha> >(warehouseTableID, partitionID));
                        } else if (context.protocol == "TwoPL") {
                                tbl_warehouse_vec.push_back(
					std::make_unique<TableBTreeOLC<warehouse::key, warehouse::value, warehouse::KeyComparator, warehouse::ValueComparator, MetaInitFuncTwoPL> >(warehouseTableID, partitionID));
                        } else if (context.protocol == "TwoPLPasha") {
                                tbl_warehouse_vec.push_back(
					std::make_unique<TableBTreeOLC<warehouse::key, warehouse::value, warehouse::KeyComparator, warehouse::ValueComparator, MetaInitFuncTwoPLPasha> >(warehouseTableID, partitionID));
			} else if (context.protocol != "HStore") {
				tbl_warehouse_vec.push_back(std::make_unique<TableBTreeOLC<warehouse::key, warehouse::value, warehouse::KeyComparator, warehouse::ValueComparator> >(warehouseTableID, partitionID));
			} else {
				if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON ||
				    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF ||
				    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
					tbl_warehouse_vec.push_back(
						std::make_unique<HStoreCOWTable<997, warehouse::key, warehouse::value, warehouse::KeyComparator, warehouse::ValueComparator> >(warehouseTableID, partitionID));
				} else {
					tbl_warehouse_vec.push_back(
						std::make_unique<HStoreTable<warehouse::key, warehouse::value, warehouse::KeyComparator, warehouse::ValueComparator> >(warehouseTableID, partitionID));
				}
			}

			auto districtTableID = district::tableID;
			if (context.protocol == "Sundial") {
				tbl_district_vec.push_back(
					std::make_unique<TableBTreeOLC<district::key, district::value, district::KeyComparator, district::ValueComparator, MetaInitFuncSundial> >(districtTableID, partitionID));
                        } else if (context.protocol == "SundialPasha") {
				tbl_district_vec.push_back(
					std::make_unique<TableBTreeOLC<district::key, district::value, district::KeyComparator, district::ValueComparator, MetaInitFuncSundialPasha> >(districtTableID, partitionID));
			} else if (context.protocol == "TwoPL") {
				tbl_district_vec.push_back(
					std::make_unique<TableBTreeOLC<district::key, district::value, district::KeyComparator, district::ValueComparator, MetaInitFuncTwoPL> >(districtTableID, partitionID));
                        } else if (context.protocol == "TwoPLPasha") {
				tbl_district_vec.push_back(
					std::make_unique<TableBTreeOLC<district::key, district::value, district::KeyComparator, district::ValueComparator, MetaInitFuncTwoPLPasha> >(districtTableID, partitionID));
                        } else if (context.protocol != "HStore") {
				tbl_district_vec.push_back(std::make_unique<TableBTreeOLC<district::key, district::value, district::KeyComparator, district::ValueComparator> >(districtTableID, partitionID));
			} else {
				if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON ||
				    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF ||
				    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
					tbl_district_vec.push_back(
						std::make_unique<HStoreCOWTable<997, district::key, district::value, district::KeyComparator, district::ValueComparator> >(districtTableID, partitionID));
				} else {
					tbl_district_vec.push_back(
						std::make_unique<HStoreTable<district::key, district::value, district::KeyComparator, district::ValueComparator> >(districtTableID, partitionID));
				}
			}

			auto customerTableID = customer::tableID;
			if (context.protocol == "Sundial") {
				tbl_customer_vec.push_back(
					std::make_unique<TableBTreeOLC<customer::key, customer::value, customer::KeyComparator, customer::ValueComparator, MetaInitFuncSundial> >(customerTableID, partitionID));
                        } else if (context.protocol == "SundialPasha") {
				tbl_customer_vec.push_back(
					std::make_unique<TableBTreeOLC<customer::key, customer::value, customer::KeyComparator, customer::ValueComparator, MetaInitFuncSundialPasha> >(customerTableID, partitionID));
			} else if (context.protocol == "TwoPL") {
				tbl_customer_vec.push_back(
					std::make_unique<TableBTreeOLC<customer::key, customer::value, customer::KeyComparator, customer::ValueComparator, MetaInitFuncTwoPL> >(customerTableID, partitionID));
                        } else if (context.protocol == "TwoPLPasha") {
				tbl_customer_vec.push_back(
					std::make_unique<TableBTreeOLC<customer::key, customer::value, customer::KeyComparator, customer::ValueComparator, MetaInitFuncTwoPLPasha> >(customerTableID, partitionID));
                        } else if (context.protocol != "HStore") {
				std::make_unique<TableBTreeOLC<customer::key, customer::value, customer::KeyComparator, customer::ValueComparator> >(customerTableID, partitionID);
			} else {
				if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON ||
				    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF ||
				    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
					tbl_customer_vec.push_back(
						std::make_unique<HStoreCOWTable<997, customer::key, customer::value, customer::KeyComparator, customer::ValueComparator> >(customerTableID, partitionID));
				} else {
					tbl_customer_vec.push_back(
						std::make_unique<HStoreTable<customer::key, customer::value, customer::KeyComparator, customer::ValueComparator> >(customerTableID, partitionID));
				}
			}

			auto customerNameIdxTableID = customer_name_idx::tableID;
			if (context.protocol == "Sundial") {
				tbl_customer_name_idx_vec.push_back(
					std::make_unique<TableHashMap<997, customer_name_idx::key, customer_name_idx::value, customer_name_idx::KeyComparator, customer_name_idx::ValueComparator, MetaInitFuncSundial> >(
						customerNameIdxTableID, partitionID));
                        } else if (context.protocol == "SundialPasha") {
				tbl_customer_name_idx_vec.push_back(
					std::make_unique<TableHashMap<997, customer_name_idx::key, customer_name_idx::value, customer_name_idx::KeyComparator, customer_name_idx::ValueComparator, MetaInitFuncSundialPasha> >(
						customerNameIdxTableID, partitionID));
                        } else if (context.protocol == "TwoPL") {
				tbl_customer_name_idx_vec.push_back(
					std::make_unique<TableHashMap<997, customer_name_idx::key, customer_name_idx::value, customer_name_idx::KeyComparator, customer_name_idx::ValueComparator, MetaInitFuncTwoPL> >(
						customerNameIdxTableID, partitionID));
                        } else if (context.protocol == "TwoPLPasha") {
				tbl_customer_name_idx_vec.push_back(
					std::make_unique<TableHashMap<997, customer_name_idx::key, customer_name_idx::value, customer_name_idx::KeyComparator, customer_name_idx::ValueComparator, MetaInitFuncTwoPLPasha> >(
						customerNameIdxTableID, partitionID));
			} else {
				tbl_customer_name_idx_vec.push_back(
					std::make_unique<TableHashMap<997, customer_name_idx::key, customer_name_idx::value, customer_name_idx::KeyComparator, customer_name_idx::ValueComparator> >(customerNameIdxTableID, partitionID));
			}

			auto historyTableID = history::tableID;
			if (context.protocol == "Sundial") {
				tbl_history_vec.push_back(
					std::make_unique<TableBTreeOLC<history::key, history::value, history::KeyComparator, history::ValueComparator, MetaInitFuncSundial> >(historyTableID, partitionID));
                        } else if (context.protocol == "SundialPasha") {
				tbl_history_vec.push_back(
					std::make_unique<TableBTreeOLC<history::key, history::value, history::KeyComparator, history::ValueComparator, MetaInitFuncSundialPasha> >(historyTableID, partitionID));
			} else if (context.protocol == "TwoPL") {
				tbl_history_vec.push_back(
					std::make_unique<TableBTreeOLC<history::key, history::value, history::KeyComparator, history::ValueComparator, MetaInitFuncTwoPL> >(historyTableID, partitionID));
                        } else if (context.protocol == "TwoPLPasha") {
				tbl_history_vec.push_back(
					std::make_unique<TableBTreeOLC<history::key, history::value, history::KeyComparator, history::ValueComparator, MetaInitFuncTwoPLPasha> >(historyTableID, partitionID));
                        } else if (context.protocol != "HStore") {
				tbl_history_vec.push_back(
					std::make_unique<TableBTreeOLC<history::key, history::value, history::KeyComparator, history::ValueComparator> >(historyTableID, partitionID));
			} else {
				if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON ||
				    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF ||
				    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
					tbl_history_vec.push_back(
						std::make_unique<HStoreCOWTable<997, history::key, history::value, history::KeyComparator, history::ValueComparator> >(historyTableID, partitionID));
				} else {
					tbl_history_vec.push_back(std::make_unique<HStoreTable<history::key, history::value, history::KeyComparator, history::ValueComparator> >(historyTableID, partitionID));
				}
			}

			auto newOrderTableID = new_order::tableID;
			if (context.protocol == "Sundial") {
				tbl_new_order_vec.push_back(
					std::make_unique<TableBTreeOLC<new_order::key, new_order::value, new_order::KeyComparator, new_order::ValueComparator, MetaInitFuncSundial> >(newOrderTableID, partitionID));
                        } else if (context.protocol == "SundialPasha") {
				tbl_new_order_vec.push_back(
					std::make_unique<TableBTreeOLC<new_order::key, new_order::value, new_order::KeyComparator, new_order::ValueComparator, MetaInitFuncSundialPasha> >(newOrderTableID, partitionID));
			} else if (context.protocol == "TwoPL") {
				tbl_new_order_vec.push_back(
					std::make_unique<TableBTreeOLC<new_order::key, new_order::value, new_order::KeyComparator, new_order::ValueComparator, MetaInitFuncTwoPL> >(newOrderTableID, partitionID));
                        } else if (context.protocol == "TwoPLPasha") {
				tbl_new_order_vec.push_back(
					std::make_unique<TableBTreeOLC<new_order::key, new_order::value, new_order::KeyComparator, new_order::ValueComparator, MetaInitFuncTwoPLPasha> >(newOrderTableID, partitionID));
                        } else if (context.protocol != "HStore") {
				tbl_new_order_vec.push_back(
					std::make_unique<TableBTreeOLC<new_order::key, new_order::value, new_order::KeyComparator, new_order::ValueComparator> >(newOrderTableID, partitionID));
			} else {
				if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON ||
				    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF ||
				    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
					tbl_new_order_vec.push_back(
						std::make_unique<HStoreCOWTable<997, new_order::key, new_order::value, new_order::KeyComparator, new_order::ValueComparator> >(newOrderTableID, partitionID));
				} else {
					tbl_new_order_vec.push_back(
						std::make_unique<HStoreTable<new_order::key, new_order::value, new_order::KeyComparator, new_order::ValueComparator> >(newOrderTableID, partitionID));
				}
			}

			auto orderTableID = order::tableID;
			if (context.protocol == "Sundial") {
				tbl_order_vec.push_back(
					std::make_unique<TableBTreeOLC<order::key, order::value, order::KeyComparator, order::ValueComparator, MetaInitFuncSundial> >(orderTableID, partitionID));
                        } else if (context.protocol == "SundialPasha") {
				tbl_order_vec.push_back(
					std::make_unique<TableBTreeOLC<order::key, order::value, order::KeyComparator, order::ValueComparator, MetaInitFuncSundialPasha> >(orderTableID, partitionID));
			} else if (context.protocol == "TwoPL") {
				tbl_order_vec.push_back(
					std::make_unique<TableBTreeOLC<order::key, order::value, order::KeyComparator, order::ValueComparator, MetaInitFuncTwoPL> >(orderTableID, partitionID));
                        } else if (context.protocol == "TwoPLPasha") {
				tbl_order_vec.push_back(
					std::make_unique<TableBTreeOLC<order::key, order::value, order::KeyComparator, order::ValueComparator, MetaInitFuncTwoPLPasha> >(orderTableID, partitionID));
                        } else if (context.protocol != "HStore") {
				tbl_order_vec.push_back(
					std::make_unique<TableBTreeOLC<order::key, order::value, order::KeyComparator, order::ValueComparator> >(orderTableID, partitionID));
			} else {
				if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON ||
				    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF ||
				    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
					tbl_order_vec.push_back(std::make_unique<HStoreCOWTable<997, order::key, order::value, order::KeyComparator, order::ValueComparator> >(orderTableID, partitionID));
				} else {
					tbl_order_vec.push_back(std::make_unique<HStoreTable<order::key, order::value, order::KeyComparator, order::ValueComparator> >(orderTableID, partitionID));
				}
			}

                        auto orderCustTableID = order_customer::tableID;
			if (context.protocol == "Sundial") {
				tbl_order_cust_vec.push_back(
					std::make_unique<TableBTreeOLC<order_customer::key, order_customer::value, order_customer::KeyComparator, order_customer::ValueComparator, MetaInitFuncSundial> >(orderCustTableID, partitionID));
                        } else if (context.protocol == "SundialPasha") {
				tbl_order_cust_vec.push_back(
					std::make_unique<TableBTreeOLC<order_customer::key, order_customer::value, order_customer::KeyComparator, order_customer::ValueComparator, MetaInitFuncSundialPasha> >(orderCustTableID, partitionID));
                        } else if (context.protocol == "TwoPL") {
				tbl_order_cust_vec.push_back(
					std::make_unique<TableBTreeOLC<order_customer::key, order_customer::value, order_customer::KeyComparator, order_customer::ValueComparator, MetaInitFuncTwoPL> >(orderCustTableID, partitionID));
			} else if (context.protocol == "TwoPLPasha") {
				tbl_order_cust_vec.push_back(
					std::make_unique<TableBTreeOLC<order_customer::key, order_customer::value, order_customer::KeyComparator, order_customer::ValueComparator, MetaInitFuncTwoPLPasha> >(orderCustTableID, partitionID));
                        } else if (context.protocol != "HStore") {
				tbl_order_cust_vec.push_back(
					std::make_unique<TableBTreeOLC<order_customer::key, order_customer::value, order_customer::KeyComparator, order_customer::ValueComparator> >(orderCustTableID, partitionID));
			} else {
				if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON ||
				    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF ||
				    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
					tbl_order_cust_vec.push_back(std::make_unique<HStoreCOWTable<997, order_customer::key, order_customer::value, order_customer::KeyComparator, order_customer::ValueComparator> >(orderCustTableID, partitionID));
				} else {
					tbl_order_cust_vec.push_back(std::make_unique<HStoreTable<order_customer::key, order_customer::value, order_customer::KeyComparator, order_customer::ValueComparator> >(orderCustTableID, partitionID));
				}
			}

			auto orderLineTableID = order_line::tableID;
			if (context.protocol == "Sundial") {
				tbl_order_line_vec.push_back(
					std::make_unique<TableBTreeOLC<order_line::key, order_line::value, order_line::KeyComparator, order_line::ValueComparator, MetaInitFuncSundial> >(orderLineTableID, partitionID));
                        } else if (context.protocol == "SundialPasha") {
				tbl_order_line_vec.push_back(
					std::make_unique<TableBTreeOLC<order_line::key, order_line::value, order_line::KeyComparator, order_line::ValueComparator, MetaInitFuncSundialPasha> >(orderLineTableID, partitionID));
			} else if (context.protocol == "TwoPL") {
				tbl_order_line_vec.push_back(
					std::make_unique<TableBTreeOLC<order_line::key, order_line::value, order_line::KeyComparator, order_line::ValueComparator, MetaInitFuncTwoPL> >(orderLineTableID, partitionID));
                        } else if (context.protocol == "TwoPLPasha") {
				tbl_order_line_vec.push_back(
					std::make_unique<TableBTreeOLC<order_line::key, order_line::value, order_line::KeyComparator, order_line::ValueComparator, MetaInitFuncTwoPLPasha> >(orderLineTableID, partitionID));
                        } else if (context.protocol != "HStore") {
				tbl_order_line_vec.push_back(
					std::make_unique<TableBTreeOLC<order_line::key, order_line::value, order_line::KeyComparator, order_line::ValueComparator> >(orderLineTableID, partitionID));
			} else {
				if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON ||
				    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF ||
				    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
					tbl_order_line_vec.push_back(
						std::make_unique<HStoreCOWTable<997, order_line::key, order_line::value, order_line::KeyComparator, order_line::ValueComparator> >(orderLineTableID, partitionID));
				} else {
					tbl_order_line_vec.push_back(
						std::make_unique<HStoreTable<order_line::key, order_line::value, order_line::KeyComparator, order_line::ValueComparator> >(orderLineTableID, partitionID));
				}
			}

			auto stockTableID = stock::tableID;
			if (context.protocol == "Sundial") {
				tbl_stock_vec.push_back(
					std::make_unique<TableBTreeOLC<stock::key, stock::value, stock::KeyComparator, stock::ValueComparator, MetaInitFuncSundial> >(stockTableID, partitionID));
                        } else if (context.protocol == "SundialPasha") {
				tbl_stock_vec.push_back(
					std::make_unique<TableBTreeOLC<stock::key, stock::value, stock::KeyComparator, stock::ValueComparator, MetaInitFuncSundialPasha> >(stockTableID, partitionID));
			} else if (context.protocol == "TwoPL") {
				tbl_stock_vec.push_back(
					std::make_unique<TableBTreeOLC<stock::key, stock::value, stock::KeyComparator, stock::ValueComparator, MetaInitFuncTwoPL> >(stockTableID, partitionID));
                        } else if (context.protocol == "TwoPLPasha") {
				tbl_stock_vec.push_back(
					std::make_unique<TableBTreeOLC<stock::key, stock::value, stock::KeyComparator, stock::ValueComparator, MetaInitFuncTwoPLPasha> >(stockTableID, partitionID));
                        } else if (context.protocol != "HStore") {
				tbl_stock_vec.push_back(std::make_unique<TableBTreeOLC<stock::key, stock::value, stock::KeyComparator, stock::ValueComparator> >(stockTableID, partitionID));
			} else {
				if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON ||
				    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF ||
				    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
					tbl_stock_vec.push_back(std::make_unique<HStoreCOWTable<997, stock::key, stock::value, stock::KeyComparator, stock::ValueComparator> >(stockTableID, partitionID));
				} else {
					tbl_stock_vec.push_back(std::make_unique<HStoreTable<stock::key, stock::value, stock::KeyComparator, stock::ValueComparator> >(stockTableID, partitionID));
				}
			}
		}

		auto itemTableID = item::tableID;
		if (context.protocol == "Sundial") {
			tbl_item_vec.push_back(std::make_unique<TableBTreeOLC<item::key, item::value, item::KeyComparator, item::ValueComparator, MetaInitFuncSundial> >(itemTableID, 0));
                } else if (context.protocol == "SundialPasha") {
			tbl_item_vec.push_back(std::make_unique<TableBTreeOLC<item::key, item::value, item::KeyComparator, item::ValueComparator, MetaInitFuncSundialPasha> >(itemTableID, 0));
                } else if (context.protocol == "TwoPL") {
			tbl_item_vec.push_back(std::make_unique<TableBTreeOLC<item::key, item::value, item::KeyComparator, item::ValueComparator, MetaInitFuncTwoPL> >(itemTableID, 0));
                } else if (context.protocol == "TwoPLPasha") {
			tbl_item_vec.push_back(std::make_unique<TableBTreeOLC<item::key, item::value, item::KeyComparator, item::ValueComparator, MetaInitFuncTwoPLPasha> >(itemTableID, 0));
		} else if (context.protocol != "HStore") {
			tbl_item_vec.push_back(std::make_unique<TableBTreeOLC<item::key, item::value, item::KeyComparator, item::ValueComparator> >(itemTableID, 0));
		} else {
			if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF ||
			    context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
				tbl_item_vec.push_back(std::make_unique<HStoreCOWTable<997, item::key, item::value, item::KeyComparator, item::ValueComparator> >(itemTableID, 0));
			} else {
				tbl_item_vec.push_back(std::make_unique<HStoreTable<item::key, item::value, item::KeyComparator, item::ValueComparator> >(itemTableID, 0));
			}
		}

		// there are 11 tables in tpcc
		tbl_vecs.resize(11);

		auto tFunc = [](std::unique_ptr<ITable> &table) { return table.get(); };

		std::transform(tbl_warehouse_vec.begin(), tbl_warehouse_vec.end(), std::back_inserter(tbl_vecs[0]), tFunc);
		std::transform(tbl_district_vec.begin(), tbl_district_vec.end(), std::back_inserter(tbl_vecs[1]), tFunc);
		std::transform(tbl_customer_vec.begin(), tbl_customer_vec.end(), std::back_inserter(tbl_vecs[2]), tFunc);
		std::transform(tbl_customer_name_idx_vec.begin(), tbl_customer_name_idx_vec.end(), std::back_inserter(tbl_vecs[3]), tFunc);
		std::transform(tbl_history_vec.begin(), tbl_history_vec.end(), std::back_inserter(tbl_vecs[4]), tFunc);
		std::transform(tbl_new_order_vec.begin(), tbl_new_order_vec.end(), std::back_inserter(tbl_vecs[5]), tFunc);
		std::transform(tbl_order_vec.begin(), tbl_order_vec.end(), std::back_inserter(tbl_vecs[6]), tFunc);
                std::transform(tbl_order_cust_vec.begin(), tbl_order_cust_vec.end(), std::back_inserter(tbl_vecs[7]), tFunc);
		std::transform(tbl_order_line_vec.begin(), tbl_order_line_vec.end(), std::back_inserter(tbl_vecs[8]), tFunc);
		std::transform(tbl_item_vec.begin(), tbl_item_vec.end(), std::back_inserter(tbl_vecs[9]), tFunc);
		std::transform(tbl_stock_vec.begin(), tbl_stock_vec.end(), std::back_inserter(tbl_vecs[10]), tFunc);

		DLOG(INFO) << "hash tables created in " << std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - now).count()
			   << " milliseconds.";

		using std::placeholders::_1;
		initTables(
			"warehouse", [this](std::size_t partitionID) { warehouseInit(partitionID); }, partitionNum, threadsNum, partitioner.get());
		initTables(
			"district", [this](std::size_t partitionID) { districtInit(partitionID); }, partitionNum, threadsNum, partitioner.get());
		initTables(
			"customer", [this](std::size_t partitionID) { customerInit(partitionID); }, partitionNum, threadsNum, partitioner.get());
		initTables(
			"customer_name_idx", [this](std::size_t partitionID) { customerNameIdxInit(partitionID); }, partitionNum, threadsNum,
			partitioner.get());
		initTables("history",
			   [this](std::size_t partitionID) { historyInit(partitionID); },
			   partitionNum, threadsNum, partitioner.get());
		initTables("new_order",
			   [this](std::size_t partitionID) { newOrderInit(partitionID); },
			   partitionNum, threadsNum, partitioner.get());
		initTables("order",
			   [this](std::size_t partitionID) { orderInit(partitionID); },
			   partitionNum, threadsNum, partitioner.get());
                initTables("order_cust",
			   [this](std::size_t partitionID) { orderCustInit(partitionID); },
			   partitionNum, threadsNum, partitioner.get());
		initTables("order_line",
			   [this](std::size_t partitionID) { orderLineInit(partitionID); },
			   partitionNum, threadsNum, partitioner.get());
		initTables(
			"item", [this](std::size_t partitionID) { itemInit(partitionID); }, 1, 1, nullptr);
		initTables(
			"stock", [this](std::size_t partitionID) { stockInit(partitionID); }, partitionNum, threadsNum, partitioner.get());
	}

        void check_consistency(const Context &context)
        {
                auto partitioner = PartitionerFactory::create_partitioner(context.partitioner, context.coordinator_id, context.coordinator_num);

                // Consistency Condition 1
                // Entries in the WAREHOUSE and DISTRICT tables must satisfy the relationship:
                //      W_YTD = sum(D_YTD)
                for (auto partitionID = 0u; partitionID < context.partition_num; partitionID++) {
                        if (partitioner->is_partition_replicated_on_me(partitionID) == false) {
				continue;
			}

                        // scan the warehouse table and get W_YTD
                        ITable *warehouse_table = tbl_warehouse_vec[partitionID].get();
                        warehouse::key wh_min_key = warehouse::key(0);
                        warehouse::key wh_max_key = warehouse::key(INT32_MAX);
                        std::vector<ITable::row_entity> warehouse_scan_results;

                        auto warehouse_scan_processor = [&](const void *key, std::atomic<uint64_t> *meta_ptr, void *data_ptr, bool is_last_tuple) -> bool {
                                if (warehouse_table->compare_key(key, &wh_max_key) > 0)
                                        return true;

                                // testing is single-threaded, so it is fine to make this assertion
                                CHECK(warehouse_table->compare_key(key, &wh_min_key) >= 0);

                                ITable::row_entity cur_row(key, warehouse_table->key_size(), meta_ptr, data_ptr, warehouse_table->value_size());
                                warehouse_scan_results.push_back(cur_row);

                                return false;
                        };

                        warehouse_table->scan(&wh_min_key, warehouse_scan_processor);

                        // scan the district table and get sum(D_YTD)
                        ITable *district_table = tbl_district_vec[partitionID].get();
                        district::key district_min_key = district::key(0, 0);
                        district::key district_max_key = district::key(INT32_MAX, INT32_MAX);
                        std::vector<ITable::row_entity> district_scan_results;

                        auto district_scan_processor = [&](const void *key, std::atomic<uint64_t> *meta_ptr, void *data_ptr, bool is_last_tuple) -> bool {
                                if (district_table->compare_key(key, &district_max_key) > 0)
                                        return true;

                                // testing is single-threaded, so it is fine to make this assertion
                                CHECK(district_table->compare_key(key, &district_min_key) >= 0);

                                ITable::row_entity cur_row(key, district_table->key_size(), meta_ptr, data_ptr, district_table->value_size());
                                district_scan_results.push_back(cur_row);

                                return false;
                        };

                        district_table->scan(&district_min_key, district_scan_processor);

                        // check consistency
                        const auto warehouse_row = *reinterpret_cast<warehouse::value *>(warehouse_scan_results[0].data);
                        float W_YTD = warehouse_row.W_YTD;

                        float D_YTD_SUM = 0;
                        for (auto i = 0u; i < district_scan_results.size(); i++) {
                                const auto district_row = *reinterpret_cast<district::value *>(district_scan_results[i].data);
                                D_YTD_SUM += district_row.D_YTD;
                        }

                        CHECK(W_YTD == D_YTD_SUM);
                }

                // Consistency Condition 2 & 3
                // Entries in the DISTRICT, ORDER, and NEW-ORDER tables must satisfy the relationship:
                //      D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)
                // Entries in the NEW-ORDER table must satisfy the relationship:
                //      max(NO_O_ID) - min(NO_O_ID) + 1 = [number of rows in the NEW-ORDER table for this district]
                for (auto partitionID = 0u; partitionID < context.partition_num; partitionID++) {
                        if (partitioner->is_partition_replicated_on_me(partitionID) == false) {
				continue;
			}

                        // scan the district table and get D_NEXT_O_ID
                        ITable *district_table = tbl_district_vec[partitionID].get();
                        district::key district_min_key = district::key(0, 0);
                        district::key district_max_key = district::key(INT32_MAX, INT32_MAX);
                        std::vector<ITable::row_entity> district_scan_results;

                        auto district_scan_processor = [&](const void *key, std::atomic<uint64_t> *meta_ptr, void *data_ptr, bool is_last_tuple) -> bool {
                                if (district_table->compare_key(key, &district_max_key) > 0)
                                        return true;

                                // testing is single-threaded, so it is fine to make this assertion
                                CHECK(district_table->compare_key(key, &district_min_key) >= 0);

                                ITable::row_entity cur_row(key, district_table->key_size(), meta_ptr, data_ptr, district_table->value_size());
                                district_scan_results.push_back(cur_row);

                                return false;
                        };

                        district_table->scan(&district_min_key, district_scan_processor);

                        // scan the order table and get O_ID
                        ITable *order_table = tbl_order_vec[partitionID].get();
                        order::key order_min_key = order::key(0, 0, 0);
                        order::key order_max_key = order::key(INT32_MAX, INT32_MAX, INT32_MAX);
                        std::vector<ITable::row_entity> order_scan_results;

                        auto order_scan_processor = [&](const void *key, std::atomic<uint64_t> *meta_ptr, void *data_ptr, bool is_last_tuple) -> bool {
                                if (order_table->compare_key(key, &order_max_key) > 0)
                                        return true;

                                // testing is single-threaded, so it is fine to make this assertion
                                CHECK(order_table->compare_key(key, &order_min_key) >= 0);

                                ITable::row_entity cur_row(key, order_table->key_size(), meta_ptr, data_ptr, order_table->value_size());
                                order_scan_results.push_back(cur_row);

                                return false;
                        };

                        order_table->scan(&order_min_key, order_scan_processor);

                        // scan the new_order table and get NO_O_ID
                        ITable *new_order_table = tbl_new_order_vec[partitionID].get();
                        new_order::key new_order_min_key = new_order::key(0, 0, 0);
                        new_order::key new_order_max_key = new_order::key(INT32_MAX, INT32_MAX, INT32_MAX);
                        std::vector<ITable::row_entity> new_order_scan_results;

                        auto new_order_scan_processor = [&](const void *key, std::atomic<uint64_t> *meta_ptr, void *data_ptr, bool is_last_tuple) -> bool {
                                if (new_order_table->compare_key(key, &new_order_max_key) > 0)
                                        return true;

                                // testing is single-threaded, so it is fine to make this assertion
                                CHECK(new_order_table->compare_key(key, &new_order_min_key) >= 0);

                                ITable::row_entity cur_row(key, new_order_table->key_size(), meta_ptr, data_ptr, new_order_table->value_size());
                                new_order_scan_results.push_back(cur_row);

                                return false;
                        };

                        new_order_table->scan(&new_order_min_key, new_order_scan_processor);

                        // check consistency
                        uint64_t D_NEXT_O_ID_vec[DISTRICT_PER_WAREHOUSE] = { 0 };
                        uint64_t max_O_ID_vec[DISTRICT_PER_WAREHOUSE] = { 0 };
                        uint64_t max_NO_O_ID_vec[DISTRICT_PER_WAREHOUSE] = { 0 };
                        uint64_t min_NO_O_ID_vec[DISTRICT_PER_WAREHOUSE] = { 0 };
                        uint64_t new_order_len_vec[DISTRICT_PER_WAREHOUSE] = { 0 };

                        for (int i = 0; i < DISTRICT_PER_WAREHOUSE; i++) {
                                min_NO_O_ID_vec[i] = INT32_MAX;
                        }

                        for (int i = 0; i < district_scan_results.size(); i++) {
                                const auto district_row = *reinterpret_cast<district::value *>(district_scan_results[i].data);
                                D_NEXT_O_ID_vec[i] = district_row.D_NEXT_O_ID;
                        }

                        for (int i = 0; i < order_scan_results.size(); i++) {
                                const auto order_key = *reinterpret_cast<order::key *>(order_scan_results[i].key);
                                if (order_key.O_D_ID == 0 || order_key.O_D_ID == INT32_MAX) {
                                        continue;
                                }
                                CHECK(order_key.O_D_ID >= 1 && order_key.O_D_ID <= 10);
                                const uint64_t cur_district_index = order_key.O_D_ID - 1;
                                if (order_key.O_ID > max_O_ID_vec[cur_district_index]) {
                                        max_O_ID_vec[cur_district_index] = order_key.O_ID;
                                }
                        }

                        for (int i = 0; i < new_order_scan_results.size(); i++) {
                                const auto new_order_key = *reinterpret_cast<new_order::key *>(new_order_scan_results[i].key);
                                if (new_order_key.NO_D_ID == 0 || new_order_key.NO_D_ID == INT32_MAX) {
                                        continue;
                                }
                                CHECK(new_order_key.NO_D_ID >= 1 && new_order_key.NO_D_ID <= 10);
                                const uint64_t cur_district_index = new_order_key.NO_D_ID - 1;
                                if (new_order_key.NO_O_ID > max_NO_O_ID_vec[cur_district_index]) {
                                        max_NO_O_ID_vec[cur_district_index] = new_order_key.NO_O_ID;
                                }
                                if (new_order_key.NO_O_ID < min_NO_O_ID_vec[cur_district_index]) {
                                        min_NO_O_ID_vec[cur_district_index] = new_order_key.NO_O_ID;
                                }
                                new_order_len_vec[cur_district_index]++;
                        }

                        for (int i = 0; i < DISTRICT_PER_WAREHOUSE; i++) {
                                CHECK(D_NEXT_O_ID_vec[i] == max_O_ID_vec[i] + 1);
                                CHECK(D_NEXT_O_ID_vec[i] == max_NO_O_ID_vec[i] + 1);
                                CHECK((max_NO_O_ID_vec[i] - min_NO_O_ID_vec[i] + 1) == new_order_len_vec[i]);
                        }
                }

                LOG(INFO) << "TPC-C consistency check passed!";
        }

	void apply_operation(const Operation &operation)
	{
		Decoder dec(operation.data);
		bool is_neworder;
		dec >> is_neworder;

		if (is_neworder) {
			// district
			auto districtTableID = district::tableID;
			district::key district_key;
			dec >> district_key.D_W_ID >> district_key.D_ID;

			auto row = tbl_district_vec[district_key.D_W_ID - 1]->search(&district_key);
			MetaDataType &tid = *std::get<0>(row);
			tid.store(operation.tid);
			district::value &district_value = *static_cast<district::value *>(std::get<1>(row));
			dec >> district_value.D_NEXT_O_ID;

			// stock
			auto stockTableID = stock::tableID;
			while (dec.size() > 0) {
				stock::key stock_key;
				dec >> stock_key.S_W_ID >> stock_key.S_I_ID;

				auto row = tbl_stock_vec[stock_key.S_W_ID - 1]->search(&stock_key);
				MetaDataType &tid = *std::get<0>(row);
				tid.store(operation.tid);
				stock::value &stock_value = *static_cast<stock::value *>(std::get<1>(row));

				dec >> stock_value.S_QUANTITY >> stock_value.S_YTD >> stock_value.S_ORDER_CNT >> stock_value.S_REMOTE_CNT;
			}
		} else {
			{
				// warehouse
				auto warehouseTableID = warehouse::tableID;
				warehouse::key warehouse_key;
				dec >> warehouse_key.W_ID;

				auto row = tbl_warehouse_vec[warehouse_key.W_ID - 1]->search(&warehouse_key);
				MetaDataType &tid = *std::get<0>(row);
				tid.store(operation.tid);

				warehouse::value &warehouse_value = *static_cast<warehouse::value *>(std::get<1>(row));

				dec >> warehouse_value.W_YTD;
			}

			{
				// district
				auto districtTableID = district::tableID;
				district::key district_key;
				dec >> district_key.D_W_ID >> district_key.D_ID;

				auto row = tbl_district_vec[district_key.D_W_ID - 1]->search(&district_key);
				MetaDataType &tid = *std::get<0>(row);
				tid.store(operation.tid);

				district::value &district_value = *static_cast<district::value *>(std::get<1>(row));

				dec >> district_value.D_YTD;
			}

			{
				// custoemer
				auto customerTableID = customer::tableID;
				customer::key customer_key;
				dec >> customer_key.C_W_ID >> customer_key.C_D_ID >> customer_key.C_ID;

				auto row = tbl_customer_vec[customer_key.C_W_ID - 1]->search(&customer_key);
				MetaDataType &tid = *std::get<0>(row);
				tid.store(operation.tid);

				customer::value &customer_value = *static_cast<customer::value *>(std::get<1>(row));

				char C_DATA[501];
				const char *old_C_DATA = customer_value.C_DATA.c_str();
				uint32_t total_written;
				dec >> total_written;
				dec.read_n_bytes(C_DATA, total_written);
				std::memcpy(C_DATA + total_written, old_C_DATA, 500 - total_written);
				C_DATA[500] = 0;
				customer_value.C_DATA.assign(C_DATA);

				dec >> customer_value.C_BALANCE >> customer_value.C_YTD_PAYMENT >> customer_value.C_PAYMENT_CNT;
			}
		}
	}

	void start_checkpoint_process(const std::vector<int> &partitions)
	{
		static thread_local std::vector<char> checkpoint_buffer;
		checkpoint_buffer.reserve(8 * 1024 * 1024);
		const std::size_t write_buffer_threshold = 128 * 1024;
		for (auto partitionID : partitions) {
			for (std::size_t i = 0; i < tbl_vecs.size(); ++i) {
				if (i == 3 || i == 8)
					continue; // No need to checkpoint readonly customer_name_idx / item_table
				ITable *table = find_table(i, partitionID);
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
								this->checkpoint_file_writer->write(&checkpoint_buffer[0], checkpoint_buffer.size(), false, std::chrono::steady_clock::now());
								checkpoint_buffer.clear();
							}
							// checkpoint_buffer.clear();
						});
				});
			}
		}
	}

	bool checkpoint_work_finished(const std::vector<int> &partitions)
	{
		for (auto partitionID : partitions) {
			for (std::size_t i = 0; i < tbl_vecs.size(); ++i) {
				if (i == 3 || i == 8)
					continue; // No need to checkpoint readonly customer_name_idx / item_table
				ITable *table = find_table(i, partitionID);
				if (table->cow_dump_finished() == false)
					return false;
			}
		}
		return true;
	}

	void stop_checkpoint_process(const std::vector<int> &partitions)
	{
		for (auto partitionID : partitions) {
			for (std::size_t i = 0; i < tbl_vecs.size(); ++i) {
				if (i == 3 || i == 8)
					continue; // No need to checkpoint readonly customer_name_idx / item_table
				ITable *table = find_table(i, partitionID);
				auto cleanup_work = table->turn_off_cow();
				threadpools[partitionID % 6]->enqueue(cleanup_work);
			}
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

                        // auto warehouseTableID = warehouse::tableID;
                        // cxl_tbl_vecs[warehouseTableID].resize(partitionNum);
                        // CCHashTable *warehouse_cxl_hashtables = reinterpret_cast<CCHashTable *>(cxl_memory.cxlalloc_malloc_wrapper(
                        //                 sizeof(CCHashTable) * partitionNum, CXLMemory::INDEX_ALLOCATION));
                        // for (int i = 0; i < partitionNum; i++) {
                        //         CCHashTable *cxl_table = &warehouse_cxl_hashtables[i];
                        //         new(cxl_table) CCHashTable(cxl_hashtable_bkt_cnt);
                        //         cxl_table_ptrs[warehouseTableID * partitionNum + i] = reinterpret_cast<void *>(cxl_table);
                        //         cxl_tbl_vecs[warehouseTableID][i] = new CXLTableHashMap<warehouse::key>(cxl_table, warehouseTableID, i);
                        // }

                        // auto districtTableID = district::tableID;
                        // cxl_tbl_vecs[districtTableID].resize(partitionNum);
                        // CCHashTable *district_cxl_hashtables = reinterpret_cast<CCHashTable *>(cxl_memory.cxlalloc_malloc_wrapper(
                        //                 sizeof(CCHashTable) * partitionNum, CXLMemory::INDEX_ALLOCATION));
                        // for (int i = 0; i < partitionNum; i++) {
                        //         CCHashTable *cxl_table = &district_cxl_hashtables[i];
                        //         new(cxl_table) CCHashTable(cxl_hashtable_bkt_cnt);
                        //         cxl_table_ptrs[districtTableID * partitionNum + i] = reinterpret_cast<void *>(cxl_table);
                        //         cxl_tbl_vecs[districtTableID][i] = new CXLTableHashMap<district::key>(cxl_table, districtTableID, i);
                        // }

                        auto customerTableID = customer::tableID;
                        cxl_tbl_vecs[customerTableID].resize(partitionNum);
                        auto customer_cxl_btreetables = reinterpret_cast<CXLTableBTreeOLC<customer::key, customer::KeyComparator>::CXLBTree *>(cxl_memory.cxlalloc_malloc_wrapper(
                                        sizeof(CXLTableBTreeOLC<customer::key, customer::KeyComparator>::CXLBTree) * partitionNum, CXLMemory::INDEX_ALLOCATION));
                        for (int i = 0; i < partitionNum; i++) {
                                auto cxl_table = &customer_cxl_btreetables[i];
                                new(cxl_table) CXLTableBTreeOLC<customer::key, customer::KeyComparator>::CXLBTree();
                                cxl_table_ptrs[customerTableID * partitionNum + i] = reinterpret_cast<void *>(cxl_table);
                                cxl_tbl_vecs[customerTableID][i] = new CXLTableBTreeOLC<customer::key, customer::KeyComparator>(cxl_table, customerTableID, i);
                        }

                        // auto customerNameIdxTableID = customer_name_idx::tableID;
                        // cxl_tbl_vecs[customerNameIdxTableID].resize(partitionNum);
                        // CCHashTable *customer_name_idx_cxl_hashtables = reinterpret_cast<CCHashTable *>(cxl_memory.cxlalloc_malloc_wrapper(
                        //                 sizeof(CCHashTable) * partitionNum, CXLMemory::INDEX_ALLOCATION));
                        // for (int i = 0; i < partitionNum; i++) {
                        //         CCHashTable *cxl_table = &customer_name_idx_cxl_hashtables[i];
                        //         new(cxl_table) CCHashTable(cxl_hashtable_bkt_cnt);
                        //         cxl_table_ptrs[customerNameIdxTableID * partitionNum + i] = reinterpret_cast<void *>(cxl_table);
                        //         cxl_tbl_vecs[customerNameIdxTableID][i] = new CXLTableHashMap<customer_name_idx::key>(cxl_table, customerNameIdxTableID, i);
                        // }

                        // auto historyTableID = history::tableID;
                        // cxl_tbl_vecs[historyTableID].resize(partitionNum);
                        // auto history_cxl_btreetables = reinterpret_cast<CXLTableBTreeOLC<history::key, history::KeyComparator>::CXLBTree *>(cxl_memory.cxlalloc_malloc_wrapper(
                        //                 sizeof(CXLTableBTreeOLC<history::key, history::KeyComparator>::CXLBTree) * partitionNum, CXLMemory::INDEX_ALLOCATION));
                        // for (int i = 0; i < partitionNum; i++) {
                        //         auto cxl_table = &history_cxl_btreetables[i];
                        //         new(cxl_table) CXLTableBTreeOLC<history::key, history::KeyComparator>::CXLBTree();
                        //         cxl_table_ptrs[historyTableID * partitionNum + i] = reinterpret_cast<void *>(cxl_table);
                        //         cxl_tbl_vecs[historyTableID][i] = new CXLTableBTreeOLC<history::key, history::KeyComparator>(cxl_table, historyTableID, i);
                        // }

                        // auto newOrderTableID = new_order::tableID;
                        // cxl_tbl_vecs[newOrderTableID].resize(partitionNum);
                        // auto new_order_cxl_btreetables = reinterpret_cast<CXLTableBTreeOLC<new_order::key, new_order::KeyComparator>::CXLBTree *>(cxl_memory.cxlalloc_malloc_wrapper(
                        //                 sizeof(CXLTableBTreeOLC<new_order::key, new_order::KeyComparator>::CXLBTree) * partitionNum, CXLMemory::INDEX_ALLOCATION));
                        // for (int i = 0; i < partitionNum; i++) {
                        //         auto cxl_table = &new_order_cxl_btreetables[i];
                        //         new(cxl_table) CXLTableBTreeOLC<new_order::key, new_order::KeyComparator>::CXLBTree();
                        //         cxl_table_ptrs[newOrderTableID * partitionNum + i] = reinterpret_cast<void *>(cxl_table);
                        //         cxl_tbl_vecs[newOrderTableID][i] = new CXLTableBTreeOLC<new_order::key, new_order::KeyComparator>(cxl_table, newOrderTableID, i);
                        // }

                        // auto orderTableID = order::tableID;
                        // cxl_tbl_vecs[orderTableID].resize(partitionNum);
                        // auto order_cxl_btreetables = reinterpret_cast<CXLTableBTreeOLC<order::key, order::KeyComparator>::CXLBTree *>(cxl_memory.cxlalloc_malloc_wrapper(
                        //                 sizeof(CXLTableBTreeOLC<order::key, order::KeyComparator>::CXLBTree) * partitionNum, CXLMemory::INDEX_ALLOCATION));
                        // for (int i = 0; i < partitionNum; i++) {
                        //         auto cxl_table = &order_cxl_btreetables[i];
                        //         new(cxl_table) CXLTableBTreeOLC<order::key, order::KeyComparator>::CXLBTree();
                        //         cxl_table_ptrs[orderTableID * partitionNum + i] = reinterpret_cast<void *>(cxl_table);
                        //         cxl_tbl_vecs[orderTableID][i] = new CXLTableBTreeOLC<order::key, order::KeyComparator>(cxl_table, orderTableID, i);
                        // }

                        // auto orderCustTableID = order_customer::tableID;
                        // cxl_tbl_vecs[orderCustTableID].resize(partitionNum);
                        // auto order_cust_cxl_btreetables = reinterpret_cast<CXLTableBTreeOLC<order_customer::key, order_customer::KeyComparator>::CXLBTree *>(cxl_memory.cxlalloc_malloc_wrapper(
                        //                 sizeof(CXLTableBTreeOLC<order_customer::key, order_customer::KeyComparator>::CXLBTree) * partitionNum, CXLMemory::INDEX_ALLOCATION));
                        // for (int i = 0; i < partitionNum; i++) {
                        //         auto cxl_table = &order_cust_cxl_btreetables[i];
                        //         new(cxl_table) CXLTableBTreeOLC<order_customer::key, order_customer::KeyComparator>::CXLBTree();
                        //         cxl_table_ptrs[orderCustTableID * partitionNum + i] = reinterpret_cast<void *>(cxl_table);
                        //         cxl_tbl_vecs[orderCustTableID][i] = new CXLTableBTreeOLC<order_customer::key, order_customer::KeyComparator>(cxl_table, orderCustTableID, i);
                        // }

                        // auto orderLineTableID = order_line::tableID;
                        // cxl_tbl_vecs[orderLineTableID].resize(partitionNum);
                        // auto order_line_cxl_btreetables = reinterpret_cast<CXLTableBTreeOLC<order_line::key, order_line::KeyComparator>::CXLBTree *>(cxl_memory.cxlalloc_malloc_wrapper(
                        //                 sizeof(CXLTableBTreeOLC<order_line::key, order_line::KeyComparator>::CXLBTree) * partitionNum, CXLMemory::INDEX_ALLOCATION));
                        // for (int i = 0; i < partitionNum; i++) {
                        //         auto cxl_table = &order_line_cxl_btreetables[i];
                        //         new(cxl_table) CXLTableBTreeOLC<order_line::key, order_line::KeyComparator>::CXLBTree();
                        //         cxl_table_ptrs[orderLineTableID * partitionNum + i] = reinterpret_cast<void *>(cxl_table);
                        //         cxl_tbl_vecs[orderLineTableID][i] = new CXLTableBTreeOLC<order_line::key, order_line::KeyComparator>(cxl_table, orderLineTableID, i);
                        // }

                        // auto itemTableID = item::tableID;
                        // cxl_tbl_vecs[itemTableID].resize(partitionNum);
                        // CCHashTable *item_cxl_hashtables = reinterpret_cast<CCHashTable *>(cxl_memory.cxlalloc_malloc_wrapper(
                        //                 sizeof(CCHashTable) * partitionNum, CXLMemory::INDEX_ALLOCATION));
                        // for (int i = 0; i < partitionNum; i++) {
                        //         CCHashTable *cxl_table = &item_cxl_hashtables[i];
                        //         new(cxl_table) CCHashTable(cxl_hashtable_bkt_cnt);
                        //         cxl_table_ptrs[itemTableID * partitionNum + i] = reinterpret_cast<void *>(cxl_table);
                        //         cxl_tbl_vecs[itemTableID][i] = new CXLTableHashMap<item::key>(cxl_table, itemTableID, i);
                        // }

                        auto stockTableID = stock::tableID;
                        cxl_tbl_vecs[stockTableID].resize(partitionNum);
                        auto stock_cxl_btreetables = reinterpret_cast<CXLTableBTreeOLC<stock::key, stock::KeyComparator>::CXLBTree *>(cxl_memory.cxlalloc_malloc_wrapper(
                                        sizeof(CXLTableBTreeOLC<stock::key, stock::KeyComparator>::CXLBTree) * partitionNum, CXLMemory::INDEX_ALLOCATION));
                        for (int i = 0; i < partitionNum; i++) {
                                auto cxl_table = &stock_cxl_btreetables[i];
                                new(cxl_table) CXLTableBTreeOLC<stock::key, stock::KeyComparator>::CXLBTree();
                                cxl_table_ptrs[stockTableID * partitionNum + i] = reinterpret_cast<void *>(cxl_table);
                                cxl_tbl_vecs[stockTableID][i] = new CXLTableBTreeOLC<stock::key, stock::KeyComparator>(cxl_table, stockTableID, i);
                        }

                        CXLMemory::commit_shared_data_initialization(CXLMemory::cxl_data_migration_root_index, cxl_table_ptrs);
                        LOG(INFO) << "TPCC initializes data migration metadata";
                } else {
                        // other hosts wait and retrieve the CXL tables
                        void *tmp = NULL;
                        CXLMemory::wait_and_retrieve_cxl_shared_data(CXLMemory::cxl_data_migration_root_index, &tmp);
                        boost::interprocess::offset_ptr<void> *cxl_table_ptrs = reinterpret_cast<boost::interprocess::offset_ptr<void> *>(tmp);

                        // auto warehouseTableID = warehouse::tableID;
                        // cxl_tbl_vecs[warehouseTableID].resize(partitionNum);
                        // for (int i = 0; i < partitionNum; i++) {
                        //         CCHashTable *cxl_table = reinterpret_cast<CCHashTable *>(cxl_table_ptrs[warehouseTableID * partitionNum + i].get());
                        //         cxl_tbl_vecs[warehouseTableID][i] = new CXLTableHashMap<warehouse::key>(cxl_table, warehouseTableID, i);
                        // }

                        // auto districtTableID = district::tableID;
                        // cxl_tbl_vecs[districtTableID].resize(partitionNum);
                        // for (int i = 0; i < partitionNum; i++) {
                        //         CCHashTable *cxl_table = reinterpret_cast<CCHashTable *>(cxl_table_ptrs[districtTableID * partitionNum + i].get());
                        //         cxl_tbl_vecs[districtTableID][i] = new CXLTableHashMap<district::key>(cxl_table, districtTableID, i);
                        // }

                        auto customerTableID = customer::tableID;
                        cxl_tbl_vecs[customerTableID].resize(partitionNum);
                        for (int i = 0; i < partitionNum; i++) {
                                auto cxl_table = reinterpret_cast<CXLTableBTreeOLC<customer::key, customer::KeyComparator>::CXLBTree *>(cxl_table_ptrs[customerTableID * partitionNum + i].get());
                                cxl_tbl_vecs[customerTableID][i] = new CXLTableBTreeOLC<customer::key, customer::KeyComparator>(cxl_table, customerTableID, i);
                        }

                        // auto customerNameIdxTableID = customer_name_idx::tableID;
                        // cxl_tbl_vecs[customerNameIdxTableID].resize(partitionNum);
                        // for (int i = 0; i < partitionNum; i++) {
                        //         CCHashTable *cxl_table = reinterpret_cast<CCHashTable *>(cxl_table_ptrs[customerNameIdxTableID * partitionNum + i].get());
                        //         cxl_tbl_vecs[customerNameIdxTableID][i] = new CXLTableHashMap<customer_name_idx::key>(cxl_table, customerNameIdxTableID, i);
                        // }

                        // auto historyTableID = history::tableID;
                        // cxl_tbl_vecs[historyTableID].resize(partitionNum);
                        // for (int i = 0; i < partitionNum; i++) {
                        //         auto cxl_table = reinterpret_cast<CXLTableBTreeOLC<history::key, history::KeyComparator>::CXLBTree *>(cxl_table_ptrs[historyTableID * partitionNum + i].get());
                        //         cxl_tbl_vecs[historyTableID][i] = new CXLTableBTreeOLC<history::key, history::KeyComparator>(cxl_table, historyTableID, i);
                        // }

                        // auto newOrderTableID = new_order::tableID;
                        // cxl_tbl_vecs[newOrderTableID].resize(partitionNum);
                        // for (int i = 0; i < partitionNum; i++) {
                        //         auto cxl_table = reinterpret_cast<CXLTableBTreeOLC<new_order::key, new_order::KeyComparator>::CXLBTree *>(cxl_table_ptrs[newOrderTableID * partitionNum + i].get());
                        //         cxl_tbl_vecs[newOrderTableID][i] = new CXLTableBTreeOLC<new_order::key, new_order::KeyComparator>(cxl_table, newOrderTableID, i);
                        // }

                        // auto orderTableID = order::tableID;
                        // cxl_tbl_vecs[orderTableID].resize(partitionNum);
                        // for (int i = 0; i < partitionNum; i++) {
                        //         auto cxl_table = reinterpret_cast<CXLTableBTreeOLC<order::key, order::KeyComparator>::CXLBTree *>(cxl_table_ptrs[orderTableID * partitionNum + i].get());
                        //         cxl_tbl_vecs[orderTableID][i] = new CXLTableBTreeOLC<order::key, order::KeyComparator>(cxl_table, orderTableID, i);
                        // }


                        // auto orderCustTableID = order_customer::tableID;
                        // cxl_tbl_vecs[orderCustTableID].resize(partitionNum);
                        // for (int i = 0; i < partitionNum; i++) {
                        //         auto cxl_table = reinterpret_cast<CXLTableBTreeOLC<order_customer::key, order_customer::KeyComparator>::CXLBTree *>(cxl_table_ptrs[orderCustTableID * partitionNum + i].get());
                        //         cxl_tbl_vecs[orderCustTableID][i] = new CXLTableBTreeOLC<order_customer::key, order_customer::KeyComparator>(cxl_table, orderCustTableID, i);
                        // }

                        // auto orderLineTableID = order_line::tableID;
                        // cxl_tbl_vecs[orderLineTableID].resize(partitionNum);
                        // for (int i = 0; i < partitionNum; i++) {
                        //         auto cxl_table = reinterpret_cast<CXLTableBTreeOLC<order_line::key, order_line::KeyComparator>::CXLBTree *>(cxl_table_ptrs[orderLineTableID * partitionNum + i].get());
                        //         cxl_tbl_vecs[orderLineTableID][i] = new CXLTableBTreeOLC<order_line::key, order_line::KeyComparator>(cxl_table, orderLineTableID, i);
                        // }

                        // auto itemTableID = item::tableID;
                        // cxl_tbl_vecs[itemTableID].resize(partitionNum);
                        // for (int i = 0; i < partitionNum; i++) {
                        //         CCHashTable *cxl_table = reinterpret_cast<CCHashTable *>(cxl_table_ptrs[itemTableID * partitionNum + i].get());
                        //         cxl_tbl_vecs[itemTableID][i] = new CXLTableHashMap<item::key>(cxl_table, itemTableID, i);
                        // }

                        auto stockTableID = stock::tableID;
                        cxl_tbl_vecs[stockTableID].resize(partitionNum);
                        for (int i = 0; i < partitionNum; i++) {
                                auto cxl_table = reinterpret_cast<CXLTableBTreeOLC<stock::key, stock::KeyComparator>::CXLBTree *>(cxl_table_ptrs[stockTableID * partitionNum + i].get());
                                cxl_tbl_vecs[stockTableID][i] = new CXLTableBTreeOLC<stock::key, stock::KeyComparator>(cxl_table, stockTableID, i);
                        }

                        LOG(INFO) << "TPCC retrieves data migration metadata";
                }

                return cxl_tbl_vecs;
        }

        void move_all_tables_into_cxl(std::function<bool(ITable *, const void *, std::tuple<MetaDataType *, void *> &, bool)> move_in_func)
        {
                // disabled for now
                CHECK(0);
        }

        void move_non_part_tables_into_cxl(std::function<bool(ITable *, const void *, std::tuple<MetaDataType *, void *> &, bool)> move_in_func)
        {
                // customer ID
                for (int i = 0; i < tbl_customer_vec.size(); i++) {
                        tbl_customer_vec[i]->move_all_into_cxl(move_in_func);
                }

                // stock
                for (int i = 0; i < tbl_stock_vec.size(); i++) {
                        tbl_stock_vec[i]->move_all_into_cxl(move_in_func);
                }
        }

    private:
	void warehouseInit(std::size_t partitionID)
	{
		Random random;
		ITable *table = tbl_warehouse_vec[partitionID].get();

		warehouse::key key;
		key.W_ID = partitionID + 1; // partitionID is from 0, W_ID is from 1

		warehouse::value value;
		value.W_NAME.assign(random.a_string(6, 10));
		value.W_STREET_1.assign(random.a_string(10, 20));
		value.W_STREET_2.assign(random.a_string(10, 20));
		value.W_CITY.assign(random.a_string(10, 20));
		value.W_STATE.assign(random.a_string(2, 2));
		value.W_ZIP.assign(random.rand_zip());
		value.W_TAX = static_cast<float>(random.uniform_dist(0, 2000)) / 10000;
		value.W_YTD = 30000;

		bool success = table->insert(&key, &value);
                CHECK(success == true);
	}

	void districtInit(std::size_t partitionID)
	{
		Random random;
		ITable *table = tbl_district_vec[partitionID].get();

		// For each row in the WAREHOUSE table, 10 rows in the DISTRICT table

		for (int i = 1; i <= DISTRICT_PER_WAREHOUSE; i++) {
			district::key key;
			key.D_W_ID = partitionID + 1;
			key.D_ID = i;

			district::value value;
			value.D_NAME.assign(random.a_string(6, 10));
			value.D_STREET_1.assign(random.a_string(10, 20));
			value.D_STREET_2.assign(random.a_string(10, 20));
			value.D_CITY.assign(random.a_string(10, 20));
			value.D_STATE.assign(random.a_string(2, 2));
			value.D_ZIP.assign(random.rand_zip());
			value.D_TAX = static_cast<float>(random.uniform_dist(0, 2000)) / 10000;
			value.D_YTD = 3000;
			value.D_NEXT_O_ID = 3001;

			bool success = table->insert(&key, &value);
                        CHECK(success == true);
		}
	}

	void customerInit(std::size_t partitionID)
	{
		Random random;
		ITable *table = tbl_customer_vec[partitionID].get();

		// For each row in the WAREHOUSE table, 10 rows in the DISTRICT table
		// For each row in the DISTRICT table, 3,000 rows in the CUSTOMER table

		for (int i = 1; i <= DISTRICT_PER_WAREHOUSE; i++) {
			for (int j = 1; j <= CUSTOMER_PER_DISTRICT; j++) {
				customer::key key;
				key.C_W_ID = partitionID + 1;
				key.C_D_ID = i;
				key.C_ID = j;

				customer::value value;
				value.C_MIDDLE.assign("OE");
				value.C_FIRST.assign(random.a_string(8, 16));
				value.C_STREET_1.assign(random.a_string(10, 20));
				value.C_STREET_2.assign(random.a_string(10, 20));
				value.C_CITY.assign(random.a_string(10, 20));
				value.C_STATE.assign(random.a_string(2, 2));
				value.C_ZIP.assign(random.rand_zip());
				value.C_PHONE.assign(random.n_string(16, 16));
				value.C_SINCE = Time::now();
				value.C_CREDIT_LIM = 50000;
				value.C_DISCOUNT = static_cast<float>(random.uniform_dist(0, 5000)) / 10000;
				value.C_BALANCE = -10;
				value.C_YTD_PAYMENT = 10;
				value.C_PAYMENT_CNT = 1;
				value.C_DELIVERY_CNT = 1;
				value.C_DATA.assign(random.a_string(300, 500));

				int last_name;

				if (j <= 1000) {
					last_name = j - 1;
				} else {
					last_name = random.non_uniform_distribution(255, 0, 999);
				}

				value.C_LAST.assign(random.rand_last_name(last_name));

				// For 10% of the rows, selected at random , C_CREDIT = "BC"

				int x = random.uniform_dist(1, 10);

				if (x == 1) {
					value.C_CREDIT.assign("BC");
				} else {
					value.C_CREDIT.assign("GC");
				}

				bool success = table->insert(&key, &value);
                                CHECK(success == true);
			}
		}
	}

	void customerNameIdxInit(std::size_t partitionID)
	{
		Random random;
		ITable *table = tbl_customer_name_idx_vec[partitionID].get();

		// For each row in the WAREHOUSE table, 10 rows in the DISTRICT table
		// For each row in the DISTRICT table, 3,000 rows in the CUSTOMER table

		ITable *customer_table = find_table(customer::tableID, partitionID);

		std::unordered_map<FixedString<16>, std::vector<std::pair<FixedString<16>, int32_t> > > last_name_to_first_names_and_c_ids;

		for (int i = 1; i <= DISTRICT_PER_WAREHOUSE; i++) {
			for (int j = 1; j <= CUSTOMER_PER_DISTRICT; j++) {
				customer::key customer_key;
				customer_key.C_W_ID = partitionID + 1;
				customer_key.C_D_ID = i;
				customer_key.C_ID = j;

				// no concurrent write, it is ok to read without validation on
				// MetaDataType
				const customer::value &customer_value = *static_cast<customer::value *>(customer_table->search_value(&customer_key));
				last_name_to_first_names_and_c_ids[customer_value.C_LAST].push_back(std::make_pair(customer_value.C_FIRST, customer_key.C_ID));
			}

			for (auto it = last_name_to_first_names_and_c_ids.begin(); it != last_name_to_first_names_and_c_ids.end(); it++) {
				auto &v = it->second;
				std::sort(v.begin(), v.end());

				// insert ceiling(n/2) to customer_last_name_idx, n starts from 1
				customer_name_idx::key cni_key(partitionID + 1, i, it->first);
				customer_name_idx::value cni_value(v[(v.size() - 1) / 2].second);
				bool success = table->insert(&cni_key, &cni_value);
                                CHECK(success == true);
			}
		}
	}

	void historyInit(std::size_t partitionID)
	{
		Random random;
		ITable *table = tbl_history_vec[partitionID].get();

		// For each row in the WAREHOUSE table, 10 rows in the DISTRICT table
		// For each row in the DISTRICT table, 3,000 rows in the CUSTOMER table
		// For each row in the CUSTOMER table, 1 row in the HISTORY table

		for (int i = 1; i <= DISTRICT_PER_WAREHOUSE; i++) {
			for (int j = 1; j <= CUSTOMER_PER_DISTRICT; j++) {
				history::key key;

				key.H_W_ID = partitionID + 1;
				key.H_D_ID = i;
				key.H_C_W_ID = partitionID + 1;
				key.H_C_D_ID = i;
				key.H_C_ID = j;
				key.H_DATE = Time::now();

				history::value value;
				value.H_AMOUNT = 10;
				value.H_DATA.assign(random.a_string(12, 24));

				bool success = table->insert(&key, &value);
                                CHECK(success == true);
			}
		}

                // insert a max key that represents the upper bound (for next-key locking)
                history::key max_key;
                history::value dummy_value;
                max_key.H_W_ID = INT32_MAX;
                max_key.H_D_ID = INT32_MAX;
                max_key.H_C_W_ID = INT32_MAX;
                max_key.H_C_D_ID = INT32_MAX;
                max_key.H_C_ID = INT32_MAX;
                max_key.H_DATE = INT64_MAX;
                bool success = table->insert(&max_key, &dummy_value);
                CHECK(success == true);
	}

	void newOrderInit(std::size_t partitionID)
	{
		Random random;
		ITable *table = tbl_new_order_vec[partitionID].get();

		// For each row in the WAREHOUSE table, 10 rows in the DISTRICT table
		// For each row in the DISTRICT table, 3,000 rows in the ORDER table
		// For each row in the ORDER table from 2101 to 3000, 1 row in the NEW_ORDER
		// table

		for (int i = 1; i <= DISTRICT_PER_WAREHOUSE; i++) {
			for (int j = 2101; j <= 3000; j++) {
				new_order::key key;
				key.NO_W_ID = partitionID + 1;
				key.NO_D_ID = i;
				key.NO_O_ID = j;

				new_order::value value;

				bool success = table->insert(&key, &value);
                                CHECK(success == true);
			}
		}

                // test correctness
                for (int i = 1; i <= DISTRICT_PER_WAREHOUSE; i++) {
                        for (int j = 2101; j <= 3000; j++) {
                                new_order::key min_key = new_order::key(partitionID + 1, i, 0);
                                new_order::key max_key = new_order::key(partitionID + 1, i, MAX_ORDER_ID);
                                std::vector<ITable::row_entity> new_order_scan_results;

                                auto local_scan_processor = [&](const void *key, std::atomic<uint64_t> *meta_ptr, void *data_ptr, bool is_last_tuple) -> bool {
                                        if (table->compare_key(key, &max_key) > 0)
                                                return true;

                                        // testing is single-threaded, so it is fine to make this assertion
                                        CHECK(table->compare_key(key, &min_key) >= 0);

                                        ITable::row_entity cur_row(key, table->key_size(), meta_ptr, data_ptr, table->value_size());
                                        new_order_scan_results.push_back(cur_row);

                                        return false;
                                };

				table->scan(&min_key, local_scan_processor);
                                CHECK(new_order_scan_results.size() == 900);

                                const auto oldest_undelivered_order_key = *reinterpret_cast<new_order::key *>(new_order_scan_results[0].key);
                                CHECK(oldest_undelivered_order_key.NO_O_ID == 2101);
                        }
                }

                // insert a max key that represents the upper bound (for next-key locking)
                new_order::key max_key;
                new_order::value dummy_value;
                max_key.NO_W_ID = INT32_MAX;
                max_key.NO_D_ID = INT32_MAX;
                max_key.NO_O_ID = INT32_MAX;
                bool success = table->insert(&max_key, &dummy_value);
                CHECK(success == true);
	}

	void orderInit(std::size_t partitionID)
	{
		Random random;
		ITable *table = tbl_order_vec[partitionID].get();

		// For each row in the WAREHOUSE table, 10 rows in the DISTRICT table
		// For each row in the DISTRICT table, 3,000 rows in the ORDER table

		std::vector<int> perm;

		for (int i = 1; i <= CUSTOMER_PER_DISTRICT; i++) {
			perm.push_back(i);
		}

		for (int i = 1; i <= DISTRICT_PER_WAREHOUSE; i++) {
			std::shuffle(perm.begin(), perm.end(), std::default_random_engine());

			for (int j = 1; j <= ORDER_PER_DISTRICT; j++) {
				order::key key;
				key.O_W_ID = partitionID + 1;
				key.O_D_ID = i;
				key.O_ID = j;

				order::value value;
				value.O_C_ID = perm[j - 1];
				value.O_ENTRY_D = Time::now();
				value.O_OL_CNT = random.uniform_dist(MIN_ORDER_LINE_PER_ORDER, MAX_ORDER_LINE_PER_ORDER);
				value.O_ALL_LOCAL = true;

				if (key.O_ID < 2101) {
					value.O_CARRIER_ID = random.uniform_dist(1, 10);
				} else {
					value.O_CARRIER_ID = 0;
				}

				bool success = table->insert(&key, &value);
                                CHECK(success == true);
			}
		}

                // insert a max key that represents the upper bound (for next-key locking)
                order::key max_key;
                order::value dummy_value;
                max_key.O_W_ID = INT32_MAX;
                max_key.O_D_ID = INT32_MAX;
                max_key.O_ID = INT32_MAX;
                bool success = table->insert(&max_key, &dummy_value);
                CHECK(success == true);
	}

        void orderCustInit(std::size_t partitionID)
	{
		Random random;
		ITable *table = tbl_order_cust_vec[partitionID].get();

		// For each row in the WAREHOUSE table, 10 rows in the DISTRICT table
		// For each row in the DISTRICT table, 3,000 rows in the ORDER table

                ITable *order_table = find_table(order::tableID, partitionID);

                for (int i = 1; i <= DISTRICT_PER_WAREHOUSE; i++) {
                        for (int j = 1; j <= ORDER_PER_DISTRICT; j++) {
                                order::key order_key;
				order_key.O_W_ID = partitionID + 1;
				order_key.O_D_ID = i;
				order_key.O_ID = j;

                                // no concurrent write, it is ok to read without validation on
				// MetaDataType
				const order::value &order_value = *static_cast<order::value *>(order_table->search_value(&order_key));

                                order_customer::key order_cust_key;
                                order_cust_key.O_W_ID = order_key.O_W_ID;
                                order_cust_key.O_D_ID = order_key.O_D_ID;
                                order_cust_key.O_C_ID = order_value.O_C_ID;
                                order_cust_key.O_ID = order_key.O_ID;

                                bool success = table->insert(&order_cust_key, &order_value);
                                CHECK(success == true);
                        }
                }

                // insert a max key that represents the upper bound (for next-key locking)
                order_customer::key max_key;
                order::value dummy_value;
                max_key.O_W_ID = INT32_MAX;
                max_key.O_D_ID = INT32_MAX;
                max_key.O_C_ID = INT32_MAX;
                max_key.O_ID = INT32_MAX;
                bool success = table->insert(&max_key, &dummy_value);
                CHECK(success == true);

                // test correctness
                for (int i = 1; i <= DISTRICT_PER_WAREHOUSE; i++) {
                        for (int j = 1; j <= CUSTOMER_PER_DISTRICT; j++) {
                                order_customer::key min_order_customer_key = order_customer::key(partitionID + 1, i, j, 0);
                                order_customer::key max_order_customer_key = order_customer::key(partitionID + 1, i, j, MAX_ORDER_ID);
                                std::vector<ITable::row_entity> order_customer_scan_results;

                                auto local_scan_processor = [&](const void *key, std::atomic<uint64_t> *meta_ptr, void *data_ptr, bool is_last_tuple) -> bool {
                                        if (table->compare_key(key, &max_order_customer_key) > 0)
                                                return true;

                                        // testing is single-threaded, so it is fine to make this assertion
                                        CHECK(table->compare_key(key, &min_order_customer_key) >= 0);

                                        ITable::row_entity cur_row(key, table->key_size(), meta_ptr, data_ptr, table->value_size());
                                        order_customer_scan_results.push_back(cur_row);

                                        return false;
                                };

                                table->scan(&min_order_customer_key, local_scan_processor);
                                CHECK(order_customer_scan_results.size() == 1);
                        }
                }
	}

	void orderLineInit(std::size_t partitionID)
	{
		Random random;
		ITable *table = tbl_order_line_vec[partitionID].get();

		// For each row in the WAREHOUSE table, 10 rows in the DISTRICT table
		// For each row in the DISTRICT table, 3,000 rows in the ORDER table
		// For each row in the ORDER table, O_OL_CNT rows in the ORDER_LINE table

		ITable *order_table = find_table(order::tableID, partitionID);

		for (int i = 1; i <= DISTRICT_PER_WAREHOUSE; i++) {
			order::key order_key;
			order_key.O_W_ID = partitionID + 1;
			order_key.O_D_ID = i;

			for (int j = 1; j <= ORDER_PER_DISTRICT; j++) {
				order_key.O_ID = j;

				// no concurrent write, it is ok to read without validation on
				// MetaDataType
				const order::value &order_value = *static_cast<order::value *>(order_table->search_value(&order_key));

				for (int k = 1; k <= order_value.O_OL_CNT; k++) {
					order_line::key key;
					key.OL_W_ID = partitionID + 1;
					key.OL_D_ID = i;
					key.OL_O_ID = j;
					key.OL_NUMBER = k;

					order_line::value value;
					value.OL_I_ID = random.uniform_dist(1, 100000);
					value.OL_SUPPLY_W_ID = partitionID + 1;
					value.OL_QUANTITY = 5;
					value.OL_DIST_INFO.assign(random.a_string(24, 24));

					if (key.OL_O_ID < 2101) {
						value.OL_DELIVERY_D = order_value.O_ENTRY_D;
						value.OL_AMOUNT = 0;
					} else {
						value.OL_DELIVERY_D = 0;
						value.OL_AMOUNT = static_cast<float>(random.uniform_dist(1, 999999)) / 100;
					}
					bool success = table->insert(&key, &value);
                                        CHECK(success == true);
				}
			}
		}

                // insert a max key that represents the upper bound (for next-key locking)
                order_line::key max_key;
                order_line::value dummy_value;
                max_key.OL_W_ID = INT32_MAX;
                max_key.OL_D_ID = INT32_MAX;
                max_key.OL_O_ID = INT32_MAX;
                max_key.OL_NUMBER = INT8_MAX;
                bool success = table->insert(&max_key, &dummy_value);
                CHECK(success == true);

                // test correctness
                for (int i = 1; i <= DISTRICT_PER_WAREHOUSE; i++) {
                        for (int j = 1; j <= ORDER_PER_DISTRICT; j++) {
                                order_line::key min_order_line_key = order_line::key(partitionID + 1, i, j, 1);
                                order_line::key max_order_line_key = order_line::key(partitionID + 1, i, j, MAX_ORDER_LINE_PER_ORDER);
                                std::vector<ITable::row_entity> order_line_scan_results;

                                auto local_scan_processor = [&](const void *key, std::atomic<uint64_t> *meta_ptr, void *data_ptr, bool is_last_tuple) -> bool {
                                        if (table->compare_key(key, &max_order_line_key) > 0)
                                                return true;

                                        // testing is single-threaded, so it is fine to make this assertion
                                        CHECK(table->compare_key(key, &min_order_line_key) >= 0);

                                        ITable::row_entity cur_row(key, table->key_size(), meta_ptr, data_ptr, table->value_size());
                                        order_line_scan_results.push_back(cur_row);

                                        return false;
                                };

                                table->scan(&min_order_line_key, local_scan_processor);
                                CHECK(order_line_scan_results.size() >= MIN_ORDER_LINE_PER_ORDER && order_line_scan_results.size() <= MAX_ORDER_LINE_PER_ORDER);
                        }
                }
	}

	void itemInit(std::size_t partitionID)
	{
		Random random;
		ITable *table = tbl_item_vec[partitionID].get();

		std::string i_original = "ORIGINAL";

		// 100,000 rows in the ITEM table

		for (int i = 1; i <= ITEM_NUM; i++) {
			item::key key;
			key.I_ID = i;

			item::value value;
			value.I_IM_ID = random.uniform_dist(1, 10000);
			value.I_NAME.assign(random.a_string(14, 24));
			value.I_PRICE = random.uniform_dist(1, 100);

			std::string i_data = random.a_string(26, 50);

			/*
			    For 10% of the rows, selected at random,
			    the string "ORIGINAL" must be held by 8 consecutive characters
			   starting at a random position within I_DATA
			*/

			int x = random.uniform_dist(1, 10);

			if (x == 1) {
				int pos = random.uniform_dist(0, i_data.length() - i_original.length());
				memcpy(&i_data[0] + pos, &i_original[0], i_original.length());
			}

			value.I_DATA.assign(i_data);

			bool success = table->insert(&key, &value);
                        CHECK(success == true);
		}

                // insert a max key that represents the upper bound (for next-key locking)
                item::key max_key;
                item::value dummy_value;
                max_key.I_ID = INT32_MAX;
                bool success = table->insert(&max_key, &dummy_value);
                CHECK(success == true);
	}

	void stockInit(std::size_t partitionID)
	{
		Random random;
		ITable *table = tbl_stock_vec[partitionID].get();

		std::string s_original = "ORIGINAL";

		// For each row in the WAREHOUSE table, 100,000 rows in the STOCK table

		for (int i = 1; i <= STOCK_PER_WAREHOUSE; i++) {
			stock::key key;
			key.S_W_ID = partitionID + 1; // partition_id from 0, W_ID from 1
			key.S_I_ID = i;

			stock::value value;

			value.S_QUANTITY = random.uniform_dist(10, 100);
			value.S_DIST_01.assign(random.a_string(24, 24));
			value.S_DIST_02.assign(random.a_string(24, 24));
			value.S_DIST_03.assign(random.a_string(24, 24));
			value.S_DIST_04.assign(random.a_string(24, 24));
			value.S_DIST_05.assign(random.a_string(24, 24));
			value.S_DIST_06.assign(random.a_string(24, 24));
			value.S_DIST_07.assign(random.a_string(24, 24));
			value.S_DIST_08.assign(random.a_string(24, 24));
			value.S_DIST_09.assign(random.a_string(24, 24));
			value.S_DIST_10.assign(random.a_string(24, 24));
			value.S_YTD = 0;
			value.S_ORDER_CNT = 0;
			value.S_REMOTE_CNT = 0;

			/*
			 For 10% of the rows, selected at random,
			 the string "ORIGINAL" must be held by 8 consecutive characters starting
			 at a random position within S_DATA
			 */

			std::string s_data = random.a_string(26, 40);

			int x = random.uniform_dist(1, 10);

			if (x == 1) {
				int pos = random.uniform_dist(0, s_data.length() - s_original.length());
				memcpy(&s_data[0] + pos, &s_original[0], s_original.length());
			}

			value.S_DATA.assign(s_data);

			bool success = table->insert(&key, &value);
                        CHECK(success == true);
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

	std::vector<std::unique_ptr<ITable> > tbl_warehouse_vec;
	std::vector<std::unique_ptr<ITable> > tbl_district_vec;
	std::vector<std::unique_ptr<ITable> > tbl_customer_vec;
	std::vector<std::unique_ptr<ITable> > tbl_customer_name_idx_vec;
	std::vector<std::unique_ptr<ITable> > tbl_history_vec;
	std::vector<std::unique_ptr<ITable> > tbl_new_order_vec;
	std::vector<std::unique_ptr<ITable> > tbl_order_vec;
	std::vector<std::unique_ptr<ITable> > tbl_order_cust_vec;
	std::vector<std::unique_ptr<ITable> > tbl_order_line_vec;
	std::vector<std::unique_ptr<ITable> > tbl_item_vec;
	std::vector<std::unique_ptr<ITable> > tbl_stock_vec;

        std::vector<std::vector<CXLTableBase *> > cxl_tbl_vecs;
};
} // namespace tpcc
} // namespace star
