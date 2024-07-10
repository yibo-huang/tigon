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

namespace star {
namespace ycsb {
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

    for (auto partitionID = 0u; partitionID < partitionNum; partitionID++) {
      auto ycsbTableID = ycsb::tableID;
      if (context.protocol == "Sundial"){
        tbl_ycsb_vec.push_back(
          std::make_unique<Table<997, ycsb::key, ycsb::value, MetaInitFuncSundial>>(ycsbTableID,
                                                                partitionID));
      } else if (context.protocol != "HStore") {
        tbl_ycsb_vec.push_back(
          std::make_unique<Table<997, ycsb::key, ycsb::value>>(ycsbTableID,
                                                                partitionID));
      } else {
        if (context.lotus_checkpoint == COW_ON_CHECKPOINT_OFF_LOGGING_ON || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_OFF || context.lotus_checkpoint == COW_ON_CHECKPOINT_ON_LOGGING_ON) {
          tbl_ycsb_vec.push_back(
          std::make_unique<HStoreCOWTable<997, ycsb::key, ycsb::value>>(ycsbTableID,
                                                                partitionID));
        } else {
          tbl_ycsb_vec.push_back(
          std::make_unique<HStoreTable<ycsb::key, ycsb::value>>(ycsbTableID,
                                                                partitionID));
        }
      }
    }

    // there is 1 table in ycsb
    tbl_vecs.resize(1);

    auto tFunc = [](std::unique_ptr<ITable> &table) { return table.get(); };

    std::transform(tbl_ycsb_vec.begin(), tbl_ycsb_vec.end(),
                   std::back_inserter(tbl_vecs[0]), tFunc);

    using std::placeholders::_1;
    initTables("ycsb",
               [&context, this](std::size_t partitionID) {
                 ycsbInit(context, partitionID);
               },
               partitionNum, threadsNum, partitioner.get());
  }

  void apply_operation(const Operation &operation) {
    CHECK(false); // not supported
  }


  void start_checkpoint_process(const std::vector<int> & partitions) {
    static thread_local std::vector<char> checkpoint_buffer;
    checkpoint_buffer.reserve(8 * 1024 * 1024);
    const std::size_t write_buffer_threshold = 128 * 1024;
    for (auto partitionID: partitions) {
      ITable *table = find_table(0, partitionID);
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

  bool checkpoint_work_finished(const std::vector<int> & partitions) {
    for (auto partitionID: partitions) {
      ITable *table = find_table(0, partitionID);
      if (table->cow_dump_finished() == false)
        return false;
    }
    return true;
  }

  void stop_checkpoint_process(const std::vector<int> & partitions) {
    for (auto partitionID: partitions) {
      ITable *table = find_table(0, partitionID);
      auto cleanup_work = table->turn_off_cow();
      threadpools[partitionID % 6]->enqueue(cleanup_work);
    }
  }

  ~Database() {
    for (size_t i = 0; i < threadpools.size(); ++i) {
      delete threadpools[i];
    }
  }
private:
  void ycsbInit(const Context &context, std::size_t partitionID) {

    Random random;
    ITable *table = tbl_ycsb_vec[partitionID].get();

    std::size_t keysPerPartition =
        context.keysPerPartition; // 5M keys per partition
    std::size_t partitionNum = context.partition_num;
    std::size_t totalKeys = keysPerPartition * partitionNum;

    if (context.strategy == PartitionStrategy::RANGE) {

      // use range partitioning

      for (auto i = partitionID * keysPerPartition;
           i < (partitionID + 1) * keysPerPartition; i++) {

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

        table->insert(&key, &value);
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

        table->insert(&key, &value);
      }
    }
  }

private:
  std::vector<ThreadPool*> threadpools;
  WALLogger * checkpoint_file_writer = nullptr;
  std::vector<std::vector<ITable *>> tbl_vecs;
  std::vector<std::unique_ptr<ITable>> tbl_ycsb_vec;
};
} // namespace ycsb
} // namespace star
