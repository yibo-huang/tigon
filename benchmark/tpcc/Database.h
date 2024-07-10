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

namespace star {
namespace tpcc {

class Database {
public:
  using MetaDataType = std::atomic<uint64_t>;
  using ContextType = Context;
  using RandomType = Random;

  ITable *find_table(std::size_t table_id, std::size_t partition_id) {
    DCHECK(table_id < tbl_vecs.size());
    DCHECK(partition_id < tbl_vecs[table_id].size());
    return tbl_vecs[table_id][partition_id];
  }

  ITable *tbl_warehouse(std::size_t partition_id) {
    DCHECK(partition_id < tbl_warehouse_vec.size());
    return tbl_warehouse_vec[partition_id].get();
  }

  ITable *tbl_district(std::size_t partition_id) {
    DCHECK(partition_id < tbl_district_vec.size());
    return tbl_district_vec[partition_id].get();
  }

  ITable *tbl_customer(std::size_t partition_id) {
    DCHECK(partition_id < tbl_customer_vec.size());
    return tbl_customer_vec[partition_id].get();
  }

  ITable *tbl_customer_name_idx(std::size_t partition_id) {
    DCHECK(partition_id < tbl_customer_name_idx_vec.size());
    return tbl_customer_name_idx_vec[partition_id].get();
  }

  ITable *tbl_history(std::size_t partition_id) {
    DCHECK(partition_id < tbl_history_vec.size());
    return tbl_history_vec[partition_id].get();
  }

  ITable *tbl_new_order(std::size_t partition_id) {
    DCHECK(partition_id < tbl_new_order_vec.size());
    return tbl_new_order_vec[partition_id].get();
  }

  ITable *tbl_order(std::size_t partition_id) {
    DCHECK(partition_id < tbl_order_vec.size());
    return tbl_order_vec[partition_id].get();
  }

  ITable *tbl_order_line(std::size_t partition_id) {
    DCHECK(partition_id < tbl_order_line_vec.size());
    return tbl_order_line_vec[partition_id].get();
  }

  ITable *tbl_item(std::size_t partition_id) {
    DCHECK(partition_id < tbl_item_vec.size());
    return tbl_item_vec[partition_id].get();
  }

  ITable *tbl_stock(std::size_t partition_id) {
    DCHECK(partition_id < tbl_stock_vec.size());
    return tbl_stock_vec[partition_id].get();
  }

  template <class InitFunc>
  void initTables(const std::string &name, InitFunc initFunc,
                  std::size_t partitionNum, std::size_t threadsNum,
                  Partitioner *partitioner) {

    std::vector<int> all_parts;

    for (auto i = 0u; i < partitionNum; i++) {
      if (partitioner == nullptr ||
          partitioner->is_partition_replicated_on_me(i)) {
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
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - now)
                     .count()
              << " milliseconds.";
  }

  void initialize(const Context &context) {
    if (context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
      for (int i = 0; i < 6; ++i) {
        threadpools.push_back(new ThreadPool(1));
      }
      checkpoint_file_writer = new SimpleWALLogger(context.lotus_checkpoint_location);
    }
    std::size_t coordinator_id = context.coordinator_id;
    std::size_t partitionNum = context.partition_num;
    std::size_t threadsNum = context.worker_num;

    auto partitioner = PartitionerFactory::create_partitioner(
        context.partitioner, coordinator_id, context.coordinator_num);

    auto now = std::chrono::steady_clock::now();

    LOG(INFO) << "creating hash tables for database...";

    for (auto partitionID = 0u; partitionID < partitionNum; partitionID++) {
      auto warehouseTableID = warehouse::tableID;
      if (context.protocol == "Sundial"){
        tbl_warehouse_vec.push_back(
          std::make_unique<Table<997, warehouse::key, warehouse::value, MetaInitFuncSundial>>(
              warehouseTableID, partitionID));
      } else if (context.protocol != "HStore") {
        tbl_warehouse_vec.push_back(
          std::make_unique<Table<997, warehouse::key, warehouse::value>>(
            warehouseTableID, partitionID));
      } else {
        if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
          tbl_warehouse_vec.push_back(
          std::make_unique<HStoreCOWTable<997, warehouse::key, warehouse::value>>(
              warehouseTableID, partitionID));
        } else {
          tbl_warehouse_vec.push_back(
          std::make_unique<HStoreTable<warehouse::key, warehouse::value>>(
              warehouseTableID, partitionID));
        }
      }
      
      auto districtTableID = district::tableID;
      if (context.protocol == "Sundial"){
        tbl_district_vec.push_back(
          std::make_unique<Table<997, district::key, district::value, MetaInitFuncSundial>>(
              districtTableID, partitionID));
      } else if (context.protocol != "HStore") {
        tbl_district_vec.push_back(
          std::make_unique<Table<997, district::key, district::value>>(
              districtTableID, partitionID));
      } else {
        if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
          tbl_district_vec.push_back(
            std::make_unique<HStoreCOWTable<997, district::key, district::value>>(
              districtTableID, partitionID));
        } else {
          tbl_district_vec.push_back(
            std::make_unique<HStoreTable<district::key, district::value>>(
              districtTableID, partitionID));
        }
      }

      auto customerTableID = customer::tableID;
      if (context.protocol == "Sundial"){
        tbl_customer_vec.push_back(
            std::make_unique<Table<997, customer::key, customer::value, MetaInitFuncSundial>>(
                customerTableID, partitionID));
      } else if (context.protocol != "HStore") {
        tbl_customer_vec.push_back(
            std::make_unique<Table<997, customer::key, customer::value>>(
                customerTableID, partitionID));
      } else {
        if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
          tbl_customer_vec.push_back(
            std::make_unique<HStoreCOWTable<997, customer::key, customer::value>>(
                customerTableID, partitionID));
        } else {
          tbl_customer_vec.push_back(
            std::make_unique<HStoreTable<customer::key, customer::value>>(
                customerTableID, partitionID));
        }
      }

      auto customerNameIdxTableID = customer_name_idx::tableID;
      if (context.protocol == "Sundial"){
        tbl_customer_name_idx_vec.push_back(
          std::make_unique<
              Table<997, customer_name_idx::key, customer_name_idx::value, MetaInitFuncSundial>>(
              customerNameIdxTableID, partitionID));
      } else {
        tbl_customer_name_idx_vec.push_back(
          std::make_unique<
              Table<997, customer_name_idx::key, customer_name_idx::value>>(
              customerNameIdxTableID, partitionID));
      }

      auto historyTableID = history::tableID;
      if (context.protocol == "Sundial"){
        tbl_history_vec.push_back(
          std::make_unique<Table<997, history::key, history::value, MetaInitFuncSundial>>(
              historyTableID, partitionID));
      } else if (context.protocol != "HStore") {
        tbl_history_vec.push_back(
          std::make_unique<Table<997, history::key, history::value>>(
              historyTableID, partitionID));
      } else {
        if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
          tbl_history_vec.push_back(
            std::make_unique<HStoreCOWTable<997, history::key, history::value>>(
              historyTableID, partitionID));
        } else {
          tbl_history_vec.push_back(
          std::make_unique<HStoreTable<history::key, history::value>>(
              historyTableID, partitionID));
        }
      }

      auto newOrderTableID = new_order::tableID;
      if (context.protocol == "Sundial"){
        tbl_new_order_vec.push_back(
          std::make_unique<Table<997, new_order::key, new_order::value, MetaInitFuncSundial>>(
              newOrderTableID, partitionID));
      } else if (context.protocol != "HStore") {
        tbl_new_order_vec.push_back(
          std::make_unique<Table<997, new_order::key, new_order::value>>(
              newOrderTableID, partitionID));
      } else {
        if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
          tbl_new_order_vec.push_back(
            std::make_unique<HStoreCOWTable<997, new_order::key, new_order::value>>(
              newOrderTableID, partitionID));
        } else {
          tbl_new_order_vec.push_back(
            std::make_unique<HStoreTable<new_order::key, new_order::value>>(
              newOrderTableID, partitionID));
        }
      }

      auto orderTableID = order::tableID;
      if (context.protocol == "Sundial"){
        tbl_order_vec.push_back(
          std::make_unique<Table<997, order::key, order::value, MetaInitFuncSundial>>(
              orderTableID, partitionID));
      } else if (context.protocol != "HStore") {
        tbl_order_vec.push_back(
          std::make_unique<Table<997, order::key, order::value>>(
              orderTableID, partitionID));
      } else {
        if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
          tbl_order_vec.push_back(
            std::make_unique<HStoreCOWTable<997, order::key, order::value>>(
              orderTableID, partitionID));
        } else {
          tbl_order_vec.push_back(
            std::make_unique<HStoreTable<order::key, order::value>>(
              orderTableID, partitionID));
        }
      }
      auto orderLineTableID = order_line::tableID;
      if (context.protocol == "Sundial"){
        tbl_order_line_vec.push_back(
          std::make_unique<Table<997, order_line::key, order_line::value, MetaInitFuncSundial>>(
              orderLineTableID, partitionID));
      } else if (context.protocol != "HStore") {
        tbl_order_line_vec.push_back(
          std::make_unique<Table<997, order_line::key, order_line::value>>(
              orderLineTableID, partitionID));
      } else {
        if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
          tbl_order_line_vec.push_back(
            std::make_unique<HStoreCOWTable<997, order_line::key, order_line::value>>(
              orderLineTableID, partitionID));
        } else {
          tbl_order_line_vec.push_back(
            std::make_unique<HStoreTable<order_line::key, order_line::value>>(
              orderLineTableID, partitionID));
        }
      }

      auto stockTableID = stock::tableID;
      if (context.protocol == "Sundial"){
        tbl_stock_vec.push_back(
          std::make_unique<Table<997, stock::key, stock::value, MetaInitFuncSundial>>(stockTableID,
                                                                 partitionID));
      } else if (context.protocol != "HStore") {
        tbl_stock_vec.push_back(
          std::make_unique<Table<997, stock::key, stock::value>>(stockTableID,
                                                                 partitionID));
      } else {
        if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
          tbl_stock_vec.push_back(
            std::make_unique<HStoreCOWTable<997, stock::key, stock::value>>(stockTableID,
                                                                 partitionID));
        } else {
          tbl_stock_vec.push_back(
            std::make_unique<HStoreTable<stock::key, stock::value>>(stockTableID,
                                                                 partitionID));
        }
      }
    }
    auto itemTableID = item::tableID;
    if (context.protocol == "Sundial"){
      tbl_item_vec.push_back(
        std::make_unique<Table<997, item::key, item::value, MetaInitFuncSundial>>(itemTableID, 0));
    } else if (context.protocol != "HStore") {
      tbl_item_vec.push_back(
        std::make_unique<Table<997, item::key, item::value>>(itemTableID, 0));
    } else {
      if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
        tbl_item_vec.push_back(
          std::make_unique<HStoreCOWTable<997, item::key, item::value>>(itemTableID, 0));
      } else {
        tbl_item_vec.push_back(
          std::make_unique<HStoreTable<item::key, item::value>>(itemTableID, 0));
      }
    }

    // there are 10 tables in tpcc
    tbl_vecs.resize(10);

    auto tFunc = [](std::unique_ptr<ITable> &table) { return table.get(); };

    std::transform(tbl_warehouse_vec.begin(), tbl_warehouse_vec.end(),
                   std::back_inserter(tbl_vecs[0]), tFunc);
    std::transform(tbl_district_vec.begin(), tbl_district_vec.end(),
                   std::back_inserter(tbl_vecs[1]), tFunc);
    std::transform(tbl_customer_vec.begin(), tbl_customer_vec.end(),
                   std::back_inserter(tbl_vecs[2]), tFunc);
    std::transform(tbl_customer_name_idx_vec.begin(),
                   tbl_customer_name_idx_vec.end(),
                   std::back_inserter(tbl_vecs[3]), tFunc);
    std::transform(tbl_history_vec.begin(), tbl_history_vec.end(),
                   std::back_inserter(tbl_vecs[4]), tFunc);
    std::transform(tbl_new_order_vec.begin(), tbl_new_order_vec.end(),
                   std::back_inserter(tbl_vecs[5]), tFunc);
    std::transform(tbl_order_vec.begin(), tbl_order_vec.end(),
                   std::back_inserter(tbl_vecs[6]), tFunc);
    std::transform(tbl_order_line_vec.begin(), tbl_order_line_vec.end(),
                   std::back_inserter(tbl_vecs[7]), tFunc);
    std::transform(tbl_item_vec.begin(), tbl_item_vec.end(),
                   std::back_inserter(tbl_vecs[8]), tFunc);
    std::transform(tbl_stock_vec.begin(), tbl_stock_vec.end(),
                   std::back_inserter(tbl_vecs[9]), tFunc);

    DLOG(INFO) << "hash tables created in "
               << std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - now)
                      .count()
               << " milliseconds.";

    using std::placeholders::_1;
    initTables("warehouse",
               [this](std::size_t partitionID) { warehouseInit(partitionID); },
               partitionNum, threadsNum, partitioner.get());
    initTables("district",
               [this](std::size_t partitionID) { districtInit(partitionID); },
               partitionNum, threadsNum, partitioner.get());
    initTables("customer",
               [this](std::size_t partitionID) { customerInit(partitionID); },
               partitionNum, threadsNum, partitioner.get());
    initTables(
        "customer_name_idx",
        [this](std::size_t partitionID) { customerNameIdxInit(partitionID); },
        partitionNum, threadsNum, partitioner.get());
    /*
    initTables("history",
               [this](std::size_t partitionID) { historyInit(partitionID); },
               partitionNum, threadsNum, partitioner.get());
    initTables("new_order",
               [this](std::size_t partitionID) { newOrderInit(partitionID); },
               partitionNum, threadsNum, partitioner.get());
    initTables("order",
               [this](std::size_t partitionID) { orderInit(partitionID); },
               partitionNum, threadsNum, partitioner.get());
    initTables("order_line",
               [this](std::size_t partitionID) { orderLineInit(partitionID); },
               partitionNum, threadsNum, partitioner.get());
    */
    initTables("item",
               [this](std::size_t partitionID) { itemInit(partitionID); }, 1, 1,
               nullptr);
    initTables("stock",
               [this](std::size_t partitionID) { stockInit(partitionID); },
               partitionNum, threadsNum, partitioner.get());
  }

  void apply_operation(const Operation &operation) {

    Decoder dec(operation.data);
    bool is_neworder;
    dec >> is_neworder;

    if (is_neworder) {
      // district
      auto districtTableID = district::tableID;
      district::key district_key;
      dec >> district_key.D_W_ID >> district_key.D_ID;

      auto row =
          tbl_district_vec[district_key.D_W_ID - 1]->search(&district_key);
      MetaDataType &tid = *std::get<0>(row);
      tid.store(operation.tid);
      district::value &district_value =
          *static_cast<district::value *>(std::get<1>(row));
      dec >> district_value.D_NEXT_O_ID;

      // stock
      auto stockTableID = stock::tableID;
      while (dec.size() > 0) {
        stock::key stock_key;
        dec >> stock_key.S_W_ID >> stock_key.S_I_ID;

        auto row = tbl_stock_vec[stock_key.S_W_ID - 1]->search(&stock_key);
        MetaDataType &tid = *std::get<0>(row);
        tid.store(operation.tid);
        stock::value &stock_value =
            *static_cast<stock::value *>(std::get<1>(row));

        dec >> stock_value.S_QUANTITY >> stock_value.S_YTD >>
            stock_value.S_ORDER_CNT >> stock_value.S_REMOTE_CNT;
      }
    } else {
      {
        // warehouse
        auto warehouseTableID = warehouse::tableID;
        warehouse::key warehouse_key;
        dec >> warehouse_key.W_ID;

        auto row =
            tbl_warehouse_vec[warehouse_key.W_ID - 1]->search(&warehouse_key);
        MetaDataType &tid = *std::get<0>(row);
        tid.store(operation.tid);

        warehouse::value &warehouse_value =
            *static_cast<warehouse::value *>(std::get<1>(row));

        dec >> warehouse_value.W_YTD;
      }

      {
        // district
        auto districtTableID = district::tableID;
        district::key district_key;
        dec >> district_key.D_W_ID >> district_key.D_ID;

        auto row =
            tbl_district_vec[district_key.D_W_ID - 1]->search(&district_key);
        MetaDataType &tid = *std::get<0>(row);
        tid.store(operation.tid);

        district::value &district_value =
            *static_cast<district::value *>(std::get<1>(row));

        dec >> district_value.D_YTD;
      }

      {
        // custoemer
        auto customerTableID = customer::tableID;
        customer::key customer_key;
        dec >> customer_key.C_W_ID >> customer_key.C_D_ID >> customer_key.C_ID;

        auto row =
            tbl_customer_vec[customer_key.C_W_ID - 1]->search(&customer_key);
        MetaDataType &tid = *std::get<0>(row);
        tid.store(operation.tid);

        customer::value &customer_value =
            *static_cast<customer::value *>(std::get<1>(row));

        char C_DATA[501];
        const char *old_C_DATA = customer_value.C_DATA.c_str();
        uint32_t total_written;
        dec >> total_written;
        dec.read_n_bytes(C_DATA, total_written);
        std::memcpy(C_DATA + total_written, old_C_DATA, 500 - total_written);
        C_DATA[500] = 0;
        customer_value.C_DATA.assign(C_DATA);

        dec >> customer_value.C_BALANCE >> customer_value.C_YTD_PAYMENT >>
            customer_value.C_PAYMENT_CNT;
      }
    }
  }


  void start_checkpoint_process(const std::vector<int> & partitions) {
    static thread_local std::vector<char> checkpoint_buffer;
    checkpoint_buffer.reserve(8 * 1024 * 1024);
    const std::size_t write_buffer_threshold = 128 * 1024;
    for (auto partitionID: partitions) {
      for (std::size_t i = 0; i < tbl_vecs.size(); ++i) {
        if (i == 3 || i == 8) continue; // No need to checkpoint readonly customer_name_idx / item_table
        ITable *table = find_table(i, partitionID);
        table->turn_on_cow();
        threadpools[partitionID % 6]->enqueue([this,write_buffer_threshold, table]() {
          table->dump_copy([&, this, table](const void * k, const void * v){
            std::size_t size_needed = table->key_size();
            auto write_idx = checkpoint_buffer.size();
            checkpoint_buffer.resize(size_needed + checkpoint_buffer.size());
            memcpy(&checkpoint_buffer[write_idx], (const char*)k, table->key_size());

            size_needed = table->value_size();
            write_idx = checkpoint_buffer.size();
            checkpoint_buffer.resize(size_needed + checkpoint_buffer.size());
            memcpy(&checkpoint_buffer[write_idx], (const char*)v, table->value_size());
          }, [&, this, write_buffer_threshold, table]() { // Called when the table is unlocked
            if (checkpoint_buffer.size() >= write_buffer_threshold) {
              this->checkpoint_file_writer->write(&checkpoint_buffer[0], checkpoint_buffer.size(), false);
              checkpoint_buffer.clear();
            }
            //checkpoint_buffer.clear();
          });
      });
      }
    }
  }

  bool checkpoint_work_finished(const std::vector<int> & partitions) {
    for (auto partitionID: partitions) {
      for (std::size_t i = 0; i < tbl_vecs.size(); ++i) {
        if (i == 3 || i == 8) continue; // No need to checkpoint readonly customer_name_idx / item_table
        ITable *table = find_table(i, partitionID);
        if (table->cow_dump_finished() == false)
          return false;
      }
    }
    return true;
  }

  void stop_checkpoint_process(const std::vector<int> & partitions) {
    for (auto partitionID: partitions) {
      for (std::size_t i = 0; i < tbl_vecs.size(); ++i) {
        if (i == 3 || i == 8) continue; // No need to checkpoint readonly customer_name_idx / item_table
        ITable *table = find_table(i, partitionID);
        auto cleanup_work = table->turn_off_cow();
        threadpools[partitionID % 6]->enqueue(cleanup_work);
      }
    }
  }

  ~Database() {
    for (size_t i = 0; i < threadpools.size(); ++i) {
      delete threadpools[i];
    }
  }
private:
  void warehouseInit(std::size_t partitionID) {

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

    table->insert(&key, &value);
  }

  void districtInit(std::size_t partitionID) {

    Random random;
    ITable *table = tbl_district_vec[partitionID].get();

    // For each row in the WAREHOUSE table, 10 rows in the DISTRICT table

    for (int i = 1; i <= 10; i++) {

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
      value.D_YTD = 30000;
      value.D_NEXT_O_ID = 3001;

      table->insert(&key, &value);
    }
  }

  void customerInit(std::size_t partitionID) {

    Random random;
    ITable *table = tbl_customer_vec[partitionID].get();

    // For each row in the WAREHOUSE table, 10 rows in the DISTRICT table
    // For each row in the DISTRICT table, 3,000 rows in the CUSTOMER table

    for (int i = 1; i <= 10; i++) {
      for (int j = 1; j <= 3000; j++) {

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
        value.C_DISCOUNT =
            static_cast<float>(random.uniform_dist(0, 5000)) / 10000;
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

        table->insert(&key, &value);
      }
    }
  }

  void customerNameIdxInit(std::size_t partitionID) {

    Random random;
    ITable *table = tbl_customer_name_idx_vec[partitionID].get();

    // For each row in the WAREHOUSE table, 10 rows in the DISTRICT table
    // For each row in the DISTRICT table, 3,000 rows in the CUSTOMER table

    ITable *customer_table = find_table(customer::tableID, partitionID);

    std::unordered_map<FixedString<16>,
                       std::vector<std::pair<FixedString<16>, int32_t>>>
        last_name_to_first_names_and_c_ids;

    for (int i = 1; i <= 10; i++) {
      for (int j = 1; j <= 3000; j++) {
        customer::key customer_key;
        customer_key.C_W_ID = partitionID + 1;
        customer_key.C_D_ID = i;
        customer_key.C_ID = j;

        // no concurrent write, it is ok to read without validation on
        // MetaDataType
        const customer::value &customer_value = *static_cast<customer::value *>(
            customer_table->search_value(&customer_key));
        last_name_to_first_names_and_c_ids[customer_value.C_LAST].push_back(
            std::make_pair(customer_value.C_FIRST, customer_key.C_ID));
      }

      for (auto it = last_name_to_first_names_and_c_ids.begin();
           it != last_name_to_first_names_and_c_ids.end(); it++) {
        auto &v = it->second;
        std::sort(v.begin(), v.end());

        // insert ceiling(n/2) to customer_last_name_idx, n starts from 1
        customer_name_idx::key cni_key(partitionID + 1, i, it->first);
        customer_name_idx::value cni_value(v[(v.size() - 1) / 2].second);
        table->insert(&cni_key, &cni_value);
      }
    }
  }

  void historyInit(std::size_t partitionID) {

    Random random;
    ITable *table = tbl_history_vec[partitionID].get();

    // For each row in the WAREHOUSE table, 10 rows in the DISTRICT table
    // For each row in the DISTRICT table, 3,000 rows in the CUSTOMER table
    // For each row in the CUSTOMER table, 1 row in the HISTORY table

    for (int i = 1; i <= 10; i++) {
      for (int j = 1; j <= 3000; j++) {

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

        table->insert(&key, &value);
      }
    }
  }

  void newOrderInit(std::size_t partitionID) {

    Random random;
    ITable *table = tbl_new_order_vec[partitionID].get();

    // For each row in the WAREHOUSE table, 10 rows in the DISTRICT table
    // For each row in the DISTRICT table, 3,000 rows in the ORDER table
    // For each row in the ORDER table from 2101 to 3000, 1 row in the NEW_ORDER
    // table

    for (int i = 1; i <= 10; i++) {
      for (int j = 2101; j <= 3000; j++) {

        new_order::key key;
        key.NO_W_ID = partitionID + 1;
        key.NO_D_ID = i;
        key.NO_O_ID = j;

        new_order::value value;

        table->insert(&key, &value);
      }
    }
  }

  void orderInit(std::size_t partitionID) {

    Random random;
    ITable *table = tbl_order_vec[partitionID].get();

    // For each row in the WAREHOUSE table, 10 rows in the DISTRICT table
    // For each row in the DISTRICT table, 3,000 rows in the ORDER table

    std::vector<int> perm;

    for (int i = 1; i <= 3000; i++) {
      perm.push_back(i);
    }

    for (int i = 1; i <= 10; i++) {

      std::shuffle(perm.begin(), perm.end(), std::default_random_engine());

      for (int j = 1; j <= 3000; j++) {

        order::key key;
        key.O_W_ID = partitionID + 1;
        key.O_D_ID = i;
        key.O_ID = j;

        order::value value;
        value.O_C_ID = perm[j - 1];
        value.O_ENTRY_D = Time::now();
        value.O_OL_CNT = random.uniform_dist(5, 15);
        value.O_ALL_LOCAL = true;

        if (key.O_ID < 2101) {
          value.O_CARRIER_ID = random.uniform_dist(1, 10);
        } else {
          value.O_CARRIER_ID = 0;
        }

        table->insert(&key, &value);
      }
    }
  }

  void orderLineInit(std::size_t partitionID) {

    Random random;
    ITable *table = tbl_order_line_vec[partitionID].get();

    // For each row in the WAREHOUSE table, 10 rows in the DISTRICT table
    // For each row in the DISTRICT table, 3,000 rows in the ORDER table
    // For each row in the ORDER table, O_OL_CNT rows in the ORDER_LINE table

    ITable *order_table = find_table(order::tableID, partitionID);

    for (int i = 1; i <= 10; i++) {

      order::key order_key;
      order_key.O_W_ID = partitionID + 1;
      order_key.O_D_ID = i;

      for (int j = 1; j <= 3000; j++) {
        order_key.O_ID = j;

        // no concurrent write, it is ok to read without validation on
        // MetaDataType
        const order::value &order_value =
            *static_cast<order::value *>(order_table->search_value(&order_key));

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
            value.OL_AMOUNT =
                static_cast<float>(random.uniform_dist(1, 999999)) / 100;
          }
          table->insert(&key, &value);
        }
      }
    }
  }

  void itemInit(std::size_t partitionID) {

    Random random;
    ITable *table = tbl_item_vec[partitionID].get();

    std::string i_original = "ORIGINAL";

    // 100,000 rows in the ITEM table

    for (int i = 1; i <= 100000; i++) {

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

      table->insert(&key, &value);
    }
  }

  void stockInit(std::size_t partitionID) {

    Random random;
    ITable *table = tbl_stock_vec[partitionID].get();

    std::string s_original = "ORIGINAL";

    // For each row in the WAREHOUSE table, 100,000 rows in the STOCK table

    for (int i = 1; i <= 100000; i++) {

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

      table->insert(&key, &value);
    }
  }

private:
  std::vector<ThreadPool*> threadpools;
  WALLogger * checkpoint_file_writer = nullptr;
  std::vector<std::vector<ITable *>> tbl_vecs;

  std::vector<std::unique_ptr<ITable>> tbl_warehouse_vec;
  std::vector<std::unique_ptr<ITable>> tbl_district_vec;
  std::vector<std::unique_ptr<ITable>> tbl_customer_vec;
  std::vector<std::unique_ptr<ITable>> tbl_customer_name_idx_vec;
  std::vector<std::unique_ptr<ITable>> tbl_history_vec;
  std::vector<std::unique_ptr<ITable>> tbl_new_order_vec;
  std::vector<std::unique_ptr<ITable>> tbl_order_vec;
  std::vector<std::unique_ptr<ITable>> tbl_order_line_vec;
  std::vector<std::unique_ptr<ITable>> tbl_item_vec;
  std::vector<std::unique_ptr<ITable>> tbl_stock_vec;
};
} // namespace tpcc
} // namespace star
