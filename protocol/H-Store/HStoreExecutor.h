//
// Created by Xinjing on 9/12/21.
//

#pragma once
#include <vector>

#include "core/Macros.h"
#include "common/DeferCode.h"
#include "core/Executor.h"
#include "protocol/H-Store/HStore.h"
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#define gettid() syscall(SYS_gettid)
#include <unordered_set>
#include <set>
namespace star {

enum class ExecutorStage {
  NORMAL,
  // The coordiantor has issued the checkpoint instruction. 
  // It waits for all executors to quiesce transaction processing.
  CHECKPOINT_START,
  // Executor enters the copy-on-write mode where foreground threads continue transaction processing on the tables in copy-on-write mode 
  // and an background thread dumps data.
  CHECKPOINT_COW,
  CHECKPOINT_COW_DUMP_FINISHED,
  // When a executor finishes dumping data, it enters this stage. 
  // The coordinator waits for all partitions finish dumping and next switches to NORMAL stage.
  CHECKPOINT_DONE,
};


std::string to_string(ExecutorStage stage) {
  switch (stage) {
    case ExecutorStage::NORMAL: return "NORMAL";
    case ExecutorStage::CHECKPOINT_START: return "CHECKPOINT_START";
    case ExecutorStage::CHECKPOINT_COW: return "CHECKPOINT_COW";
    case ExecutorStage::CHECKPOINT_COW_DUMP_FINISHED: return "CHECKPOINT_COW_DUMP_FINISHED";
    case ExecutorStage::CHECKPOINT_DONE: return "CHECKPOINT_DONE";
    default: CHECK(false);
  }
  return "";
}

enum class CheckpointAction {
  SWITCH_TO_NORMAL,
  CHECKPOINT_START_REQUEST,
  CHECKPOINT_START_RESP,
  CHECKPOINT_COW,
  CHECKPOINT_COW_DUMP_FINISHED,
  CHECKPOINT_DONE_REQUEST,
  CHECKPOINT_DONE_RESP
};

class WindowAverage {
public:
  WindowAverage(std::size_t window_size) : window_size(window_size), sum(0) {}
  void add(uint64_t v) {
    values.push_back(v);
    sum += v;
    if (values.size() > window_size) {
      sum -= values.front();
      values.pop_front();
    }
  }

  uint64_t average() {
    return sum / (values.size() + 1);
  }
private:
  std::size_t window_size;
  uint64_t sum;
  std::deque<uint64_t> values;
};

template <class Workload>
class HStoreExecutor
    : public Executor<Workload, HStore<typename Workload::DatabaseType>>

{
private:
  std::unique_ptr<Partitioner> hash_partitioner;
  std::vector<std::unique_ptr<Message>> cluster_worker_messages;
  std::vector<bool> cluster_worker_messages_filled_in;
  std::deque<int> cluster_worker_messages_ready;
  int cluster_worker_num = -1;
  int active_replica_worker_num_end = -1;
  ExecutorStage stage = ExecutorStage::NORMAL;
  Percentile<uint64_t> txn_try_times;
  Percentile<int64_t> round_concurrency;
  Percentile<int64_t> effective_round_concurrency;
  Percentile<int64_t> replica_progress_query_latency;
  Percentile<int64_t> replay_query_time;
  Percentile<int64_t> spread_time;
  Percentile<int64_t> spread_cnt;
  Percentile<int64_t> replay_time;
  Percentile<int64_t> replication_gap_after_active_replica_execution;
  Percentile<int64_t> replication_sync_comm_rounds;
  Percentile<int64_t> replication_time;
  Percentile<int64_t> txn_retries;
  Percentile<int64_t> acquire_lock_message_latency;
  Percentile<int64_t> replay_loop_time;
  Percentile<int64_t> replay_mp_concurrency;
  Percentile<int64_t> round_mp_initiated;
  Percentile<int64_t> zero_mp_initiated_cost;
  Percentile<int64_t> handle_latency;
  Percentile<int64_t> scheduling_cost;
  Percentile<int64_t> handle_msg_cnt;
  Percentile<int64_t> rtt_stat;
  Percentile<uint64_t> msg_gen_latency;
  Percentile<uint64_t> msg_send_latency;
  Percentile<uint64_t> msg_recv_latency;
  Percentile<uint64_t> msg_proc_latency;
  Percentile<uint64_t> mp_concurrency_limit;
  Percentile<double> mp_avg_abort;
  std::vector<Percentile<int64_t>> tries_latency;
  std::vector<Percentile<int64_t>> tries_prepare_time;
  std::vector<Percentile<int64_t>> tries_lock_stall_time;
  std::vector<Percentile<int64_t>> tries_execution_done_latency;
  std::vector<Percentile<int64_t>> tries_lock_response_latency;
  std::vector<Percentile<int64_t>> tries_commit_initiated_latency;
  std::vector<Percentile<int64_t>> tries_first_lock_response_latency;
  std::vector<Percentile<int64_t>> tries_first_lock_request_processed_latency;
  std::vector<Percentile<int64_t>> tries_first_lock_request_arrive_latency;
  std::vector<Percentile<int64_t>> message_processing_latency;
  std::size_t batch_per_worker;
  std::atomic<bool> ended{false};
  uint64_t worker_commit = 0;
  uint64_t sent_sp_replication_requests = 0;
  uint64_t received_sp_replication_responses = 0;
  uint64_t sent_persist_cmd_buffer_requests = 0;
  uint64_t received_persist_cmd_buffer_responses = 0;
  Percentile<int64_t> commit_interval;
  std::chrono::steady_clock::time_point last_commit;
  Percentile<int64_t> mp_arrival_interval;
  std::chrono::steady_clock::time_point last_mp_arrival;
  Percentile<int64_t> cmd_queue_time;
  Percentile<int64_t> cmd_stall_time;
  Percentile<int64_t> execution_phase_time;
  Percentile<int64_t> serialize_cmd_time;
  Percentile<int64_t> execution_after_commit_time;
  WindowAverage window_persistence_latency;
  WindowAverage window_txn_latency;
  WindowAverage window_queued_lock_req_latency;
  WindowAverage window_lock_req_latency;
  WindowAverage window_active_mps;
  int rtt_test_target_cluster_worker = -1;

  std::vector<int> transaction_lengths;
  std::vector<int> transaction_lengths_count;

  std::size_t mp_concurrency_max = 0;
public:
  using base_type = Executor<Workload, HStore<typename Workload::DatabaseType>>;

  const std::string tid_to_string(uint64_t tid) {
    return "[coordinator=" + std::to_string(tid >> 56) + ", tid=" + std::to_string(tid & ~(1ULL << 56)) + "]";
  }

  const std::string memory_usage_from_stats() {
    std::stringstream ss;
    std::size_t cmd_left = 0;
    std::size_t lock_req_left = 0;
    for (auto & q : granule_command_queues) {
      cmd_left += q.size();
    }
    for (auto & q : granule_lock_request_queues) {
      lock_req_left += q.size();
    }
    ss << "txns_replayed " << txns_replayed << ", delete_mp_txns " << delete_mp_txns << "\n"
       << "cluster_worker_messages_ready " << cluster_worker_messages_ready.size() << "\n"
       << "lock_buckets " << lock_buckets.size() << "\n"
       << "managed_partitions " << managed_partitions.size() << "\n"
       << "active_txns " << active_txns.size() << "\n"
       << "pending_txns " << pending_txns.size() << "\n"
       << "granule_command_queues " << granule_command_queues.size() << " cmd_left " << cmd_left << "\n"
       << "granule_command_queue_processing " << granule_command_queue_processing.size() << "\n"
       << "txns_candidate_for_reexecution " << txns_candidate_for_reexecution.size() << " sizeof(TransactionType) " << sizeof(TransactionType) <<  ", total bytes " << (txns_candidate_for_reexecution.size() *  sizeof(TransactionType))<<  "\n"
       << "granule_replayed_log_index " << granule_replayed_log_index.size() << "\n"
       << "granule_lock_request_queues " << granule_lock_request_queues.size() << " lock_req_left " << lock_req_left << "\n"
       << "granule_lock_reqeust_candidates " << granule_lock_reqeust_candidates.size() << "\n"
       << "lock_ids_by_this_replica_executor " << lock_ids_by_this_replica_executor.size() << "\n"
       << "checkpoint_insts " << checkpoint_insts.size() << "\n"
       << "command_buffer_data " << command_buffer_data.size() << "\n"
       << "command_buffer_outgoing_data " << command_buffer_outgoing_data.size() << "\n";
    return ss.str();
  }

  using WorkloadType = Workload;
  using ProtocolType = HStore<typename Workload::DatabaseType>;
  using DatabaseType = typename WorkloadType::DatabaseType;
  using TransactionType = typename WorkloadType::TransactionType;
  using ContextType = typename DatabaseType::ContextType;
  using RandomType = typename DatabaseType::RandomType;
  using MessageType = typename ProtocolType::MessageType;
  using MessageFactoryType = typename ProtocolType::MessageFactoryType;
  using MessageHandlerType = typename ProtocolType::MessageHandlerType;

  using StorageType = typename WorkloadType::StorageType;

  int this_cluster_worker_id;
  int replica_cluster_worker_id = -1;
  struct Lock {
    constexpr static uint64_t kReaderCntAndSignMask = 0xFFFF000000000000;
    constexpr static uint64_t kReaderCntSignMask = 0x8000000000000000;
    constexpr static uint64_t kReaderCntSignShift = 63;
    constexpr static uint64_t kReaderCntMask = 0x7FFF000000000000;
    constexpr static uint64_t kReaderCntShift = 48;
    constexpr static uint64_t kLastWriterMask = 0x0000FFFFFFFFFFFE;
    constexpr static uint64_t kLastWriterShift = 1;
    constexpr static uint64_t kWriteLockedMask = 0x0000000000000001;
    constexpr static uint64_t kWriteLockedShift = 0;

    Lock(): lock_word(0) {}

    std::string __attribute__ ((noinline)) to_string() {
      return "[write-locked=" +std::to_string(write_locked()) + ", readers=" + std::to_string(reader_cnt()) + ", last-writer=" + std::to_string(get_last_writer()) + "]";
    }

    bool reader_cnt_sign() {
      return (lock_word & kReaderCntSignMask) >> kReaderCntSignShift;
    }

    uint64_t get_word_with_reader_cnt_and_sign_cleared() {
      return lock_word & ~(kReaderCntAndSignMask);
    }
  
    int64_t reader_cnt() {
      uint64_t cnt = (lock_word & kReaderCntMask) >> kReaderCntShift;
      bool sign = reader_cnt_sign();
      return sign ? -1 * (int64_t)cnt : cnt;
    }

    // uint64_t get_word_with_reader_cnt_cleared() {
    //   return lock_word & ~(kReaderCntMask);
    // }

    void set_reader_cnt(int64_t readers) {
      uint64_t signbit = readers < 0;
      readers = std::abs(readers);
      lock_word = get_word_with_reader_cnt_and_sign_cleared() |
                 (((uint64_t)readers) << kReaderCntShift) | 
                 (signbit << kReaderCntSignShift);
    }

    void inc_reader_cnt() {
      int64_t new_readers = reader_cnt() + 1;
      set_reader_cnt(reader_cnt() + 1);
      //int64_t readers = reader_cnt() + 1;
      //lock_word = get_word_with_reader_cnt_cleared() | (readers << kReaderCntShift);
      DCHECK(reader_cnt() == new_readers);
    }

    void dec_reader_cnt() {
      // DCHECK(reader_cnt() >= 1);
      // uint64_t readers = reader_cnt() - 1;
      // lock_word = get_word_with_reader_cnt_cleared() | (readers << kReaderCntShift);
      // DCHECK(reader_cnt() == readers);
      int64_t new_readers = reader_cnt() - 1;
      set_reader_cnt(reader_cnt() - 1);
      DCHECK(reader_cnt() == new_readers);
    }

    uint64_t get_word_with_last_writer_cleared() {
      return lock_word & ~(kLastWriterMask);
    }

    uint64_t get_word_with_write_lock_cleared() {
      return lock_word & ~(kWriteLockedMask);
    }

    uint64_t get_last_writer() {
      return (lock_word & kLastWriterMask) >> kLastWriterShift;
    }

    void set_last_writer(uint64_t last_writer_tid) {
      DCHECK(last_writer_tid < (1UL << 47));
      lock_word = get_word_with_last_writer_cleared() | (last_writer_tid << kLastWriterShift);
    }

    void set_write_lock() {
      lock_word = get_word_with_write_lock_cleared() | (1 << kWriteLockedShift);
    }

    void clear_write_lock() {
      lock_word = get_word_with_write_lock_cleared();
    }

    bool write_locked() {
      return lock_word & kWriteLockedMask;
    }

    bool has_reader() {
      return reader_cnt() >= 1;
    }

    bool free() {
      return reader_cnt() == 0 && !write_locked();
    }

    uint64_t lock_word = 0;
  };

  // Two-level locking
  // First level : partition
  // Second level: granule
  std::vector<Lock> lock_buckets; // Holds the transaction id that currently is owning the lock
  std::vector<int> managed_partitions;
  bool is_replica_worker = false;
  std::unordered_map<long long, TransactionType*> active_txns;
  std::deque<TransactionType*> pending_txns;

  
  int lock_id_to_partition(int lock_id) {
    return lock_id / this->context.granules_per_partition;
  }

  int lock_id_to_granule(int lock_id) {
    return lock_id % this->context.granules_per_partition;
  }

  int to_lock_id(int partition_id, int granule_id) {
    return partition_id * this->context.granules_per_partition + granule_id;
  }

  struct lock_bitmap lock_bm;

  std::string lock_id_to_string(int lock_id) {
    return "(" + std::to_string(lock_id_to_partition(lock_id)) + "," + std::to_string(lock_id_to_granule(lock_id)) + ")";
  }
  std::string command_buffer_data;
  std::string command_buffer_outgoing_data;
  //std::deque<TxnCommand> command_buffer;
  //std::deque<TxnCommand> command_buffer_outgoing;
  
  

  /*
 *   min_replayed_log_position   <=      min_coord_txn_written_log_position   <=    next_position_in_command_log
 *               |                                        |                                       |
 *               |                                        |                                       |
 * +-------------+----------------------------------------+---------------------------------------+------------+
 * |             |                                        |                                       |            |
 * |             |                                        |                                       |            |
 * |             v                                        v                                       v            |
 * |                                                                                                           |
 * |                                                                                                           |
 * |                                                                                                           |
 * |                                           COMMAND LOG                                                     |
 * |                                                                                                           |
 * +-----------------------------------------------------------------------------------------------------------+
 */
  // For active replica
  // Next position to write to in the command log
  int64_t next_position_in_command_log = 0;
  // Commands initiated by this worker priori to this position are all written to command buffer
  int64_t minimum_coord_txn_written_log_position = -1;
  // Commands priori to this position are all executed on active/standby replica
  int64_t minimum_replayed_log_position = -1;
  // Invariant: minimum_replayed_log_position <= minimum_coord_txn_written_log_position <= next_position_in_command_log
  uint64_t get_replica_replay_log_position_requests = 0;
  uint64_t get_replica_replay_log_position_responses = 0;

  // For standby replica
  std::vector<std::deque<TxnCommandBase>> granule_command_queues; // Commands against individual partition
  std::vector<uint64_t> granule_lock_time; // Commands against individual partition
  std::vector<bool> granule_command_queue_processing;
  std::vector<int> granule_to_cmd_queue_index;
  std::deque<TransactionType*> txns_candidate_for_reexecution;
  // Records the last command's position_in_log (TxnCommand.position_in_log) that got replayed
  std::vector<int64_t> granule_replayed_log_index; 
  std::vector<std::deque<MessagePiece>> granule_lock_request_queues;
  std::deque<int> granule_lock_reqeust_candidates;
  std::size_t replica_num;
  std::vector<int> lock_ids_by_this_replica_executor;
  bool sp_parallel_exec_commit = false;
  int64_t get_minimum_replayed_log_position() {
    int64_t minv = granule_replayed_log_index[0];
    for (auto v : granule_replayed_log_index) {
      minv = std::max(minv, v);
    }
    return minv;
  }

  void add_outgoing_message(int cluster_worker_id) {
    if (cluster_worker_messages_filled_in[cluster_worker_id])
      return;
    cluster_worker_messages_filled_in[cluster_worker_id] = true;
    cluster_worker_messages_ready.push_back(cluster_worker_id);
  }

  std::string serialize_commands(std::deque<TxnCommand>::iterator it, const std::deque<TxnCommand>::iterator end) {
    std::string buffer;
    Encoder enc(buffer);
    for (; it != end; ++it) {
      const TxnCommand & cmd = *it;
      enc << cmd.tid;
      enc << cmd.is_coordinator;
      enc << cmd.is_mp;
      enc << cmd.position_in_log;
      enc << cmd.partition_id;
      enc << cmd.granule_id;
      enc << cmd.command_data.size();
      enc.write_n_bytes(cmd.command_data.data(), cmd.command_data.size());
      if (cmd.is_coordinator) {
        if (cmd.is_mp) {
          DCHECK(cmd.partition_id == -1);
        } else {
          DCHECK(cmd.partition_id != -1);
        }
      }
    }
    return buffer;
  }

  Lock& get_lock_bucket(int lock_id) {
    DCHECK(granule_to_cmd_queue_index[lock_id] != -1);
    DCHECK(granule_to_cmd_queue_index[lock_id] >= 0);
    return lock_buckets[granule_to_cmd_queue_index[lock_id]];
  }

  uint64_t & get_granule_lock_time(int lock_id) {
    DCHECK(is_replica_worker);
    DCHECK(granule_to_cmd_queue_index[lock_id] != -1);
    DCHECK(granule_to_cmd_queue_index[lock_id] >= 0);
    DCHECK(granule_to_cmd_queue_index[lock_id] < (int)granule_command_queues.size());
    return granule_lock_time[granule_to_cmd_queue_index[lock_id]];
  }


  std::deque<TxnCommandBase> & get_granule_lock_cmd_queue(int lock_id) {
    DCHECK(is_replica_worker);
    DCHECK(granule_to_cmd_queue_index[lock_id] != -1);
    DCHECK(granule_to_cmd_queue_index[lock_id] >= 0);
    DCHECK(granule_to_cmd_queue_index[lock_id] < (int)granule_command_queues.size());
    return granule_command_queues[granule_to_cmd_queue_index[lock_id]];
  }

  std::deque<MessagePiece> & get_granule_lock_request_queue(int lock_id) {
    DCHECK(is_replica_worker);
    DCHECK(granule_to_cmd_queue_index[lock_id] != -1);
    DCHECK(granule_to_cmd_queue_index[lock_id] >= 0);
    DCHECK(granule_to_cmd_queue_index[lock_id] < (int)granule_lock_request_queues.size());
    return granule_lock_request_queues[granule_to_cmd_queue_index[lock_id]];
  }

  int64_t & get_partition_last_replayed_position_in_log(int partition) {
    DCHECK(is_replica_worker);
    DCHECK(granule_to_cmd_queue_index[partition] != -1);
    DCHECK(granule_to_cmd_queue_index[partition] >= 0);
    DCHECK(granule_to_cmd_queue_index[partition] < (int)granule_command_queues.size());
    return granule_replayed_log_index[granule_to_cmd_queue_index[partition]];
  }

  struct ReplicaCheckpointMeta {
    bool cow_begin;
    bool cow_end;
    int ref_cnt;
    int checkpoint_step;
  };

  HStoreExecutor(std::size_t coordinator_id, std::size_t worker_id, DatabaseType &db,
                const ContextType &context,
                std::atomic<uint32_t> &worker_status,
                std::atomic<uint32_t> &n_complete_workers,
                std::atomic<uint32_t> &n_started_workers)
      : base_type(coordinator_id, worker_id, db, context, worker_status,
                  n_complete_workers, n_started_workers), window_persistence_latency(5000),window_txn_latency(5000),window_queued_lock_req_latency(5000), window_lock_req_latency(5000), window_active_mps(5000),
                  lock_bm(this->context.partition_num * this->context.granules_per_partition)
                   {
                        transaction_lengths.resize(context.straggler_num_txn_len);
      sp_parallel_exec_commit = context.lotus_sp_parallel_exec_commit;
      transaction_lengths_count.resize(context.straggler_num_txn_len);
      transaction_lengths[0] = 10; 
      for (size_t i = 1; i < context.straggler_num_txn_len; ++i) {
        transaction_lengths[i] = std::min(context.stragglers_total_wait_time, transaction_lengths[i - 1] * 2);
      }
      replica_num = this->partitioner->replica_num();
      tries_latency.resize(10);
      tries_prepare_time.resize(tries_latency.size());
      tries_lock_stall_time.resize(tries_latency.size());
      tries_execution_done_latency.resize(tries_latency.size());
      tries_commit_initiated_latency.resize(tries_latency.size());
      tries_lock_response_latency.resize(tries_latency.size());
      tries_first_lock_response_latency.resize(tries_latency.size());
      tries_first_lock_request_processed_latency.resize(tries_latency.size());
      tries_first_lock_request_arrive_latency.resize(tries_latency.size());
      message_processing_latency.resize((size_t)HStoreMessage::NFIELDS);
      is_replica_worker = coordinator_id < this->partitioner->num_coordinator_for_one_replica() ? false: true;
      cluster_worker_num = this->context.worker_num * this->context.coordinator_num;
      active_replica_worker_num_end = this->context.worker_num * this->partitioner->num_coordinator_for_one_replica();
      DCHECK(this->context.partition_num % this->partitioner->num_coordinator_for_one_replica() == 0);
      DCHECK(this->context.partition_num % this->context.worker_num == 0);
      DCHECK(worker_id < context.worker_num);
      this_cluster_worker_id = worker_id + coordinator_id * context.worker_num;
      std::string managed_granules_str;
      if (is_replica_worker == false) {
        hash_partitioner = PartitionerFactory::create_partitioner(
            "hash", coordinator_id, this->partitioner->num_coordinator_for_one_replica());
        for (int p = 0; p < (int)this->context.partition_num; ++p) {
          if (this_cluster_worker_id == partition_owner_cluster_worker(p, 0)) {
            managed_granules_str += std::to_string(p) + ",";
            managed_partitions.push_back(p);
            for (std::size_t j = 0; j < this->context.granules_per_partition; ++j) {
              auto lock_id = to_lock_id(p, j);
              //managed_granules_str += lock_id_to_string(lock_id) + ",";
              if (replica_num > 1) {
                auto standby_replica_cluster_worker_id = partition_owner_cluster_worker(p, 1);
                DCHECK(replica_cluster_worker_id == -1 || standby_replica_cluster_worker_id == replica_cluster_worker_id);
                replica_cluster_worker_id = standby_replica_cluster_worker_id;
              }
            }
          }
        }
        DCHECK(managed_partitions.empty() == false);
        managed_granules_str.pop_back(); // Remove last ,
        if (coordinator_id == 0) {
          rtt_test_target_cluster_worker = this_cluster_worker_id + this->context.worker_num;
        } else {
          rtt_test_target_cluster_worker = this_cluster_worker_id - this->context.worker_num;
        }
      }
      
      std::string managed_replica_granules_str;
      if (is_replica_worker) {
        if (coordinator_id == 2) {
          rtt_test_target_cluster_worker = this_cluster_worker_id + this->context.worker_num;
        } else {
          rtt_test_target_cluster_worker = this_cluster_worker_id - this->context.worker_num;
        }
        granule_to_cmd_queue_index.resize(this->context.partition_num * this->context.granules_per_partition, -1);
        DCHECK(managed_partitions.empty());
        size_t cmd_queue_idx = 0;
        for (int p = 0; p < (int)this->context.partition_num; ++p) {
          for (size_t i = 1; i < this->partitioner->replica_num(); ++i) {
            if (this_cluster_worker_id == partition_owner_cluster_worker(p, i)) {
              managed_partitions.push_back(p);
              managed_replica_granules_str += std::to_string(p) + ",";
              for (size_t j = 0; j < this->context.granules_per_partition; ++j) {
                auto lock_id = to_lock_id(p, j);
                lock_ids_by_this_replica_executor.push_back(lock_id);
                granule_command_queues.push_back(std::deque<TxnCommandBase>());
                granule_lock_time.push_back(0);
                granule_lock_request_queues.push_back(std::deque<MessagePiece>());
                granule_command_queue_processing.push_back(false);
                granule_replayed_log_index.push_back(-1);
                lock_buckets.push_back(Lock());
                granule_to_cmd_queue_index[lock_id] = cmd_queue_idx++;
                granule_command_queues.back().resize(16);
                granule_command_queues.back().clear();
                CHECK(cmd_queue_idx == granule_command_queues.size());
                CHECK(cmd_queue_idx == granule_replayed_log_index.size());
                auto active_replica_cluster_worker_id = partition_owner_cluster_worker(p, 0);
                CHECK(replica_cluster_worker_id == -1 || active_replica_cluster_worker_id == replica_cluster_worker_id);
                replica_cluster_worker_id = active_replica_cluster_worker_id;
                //managed_replica_granules_str += lock_id_to_string(lock_id) + ",";
              }
            }
          }
        }
        managed_replica_granules_str.pop_back(); // Remove last ,
      } else {
        granule_to_cmd_queue_index.resize(this->context.partition_num * this->context.granules_per_partition, -1);
        size_t cmd_queue_idx = 0;
        for (int p = 0; p < (int)this->context.partition_num; ++p) {
          if (this_cluster_worker_id == partition_owner_cluster_worker(p, 0)) {
            for (size_t j = 0; j < this->context.granules_per_partition; ++j) {
              auto lock_id = to_lock_id(p, j);
              lock_buckets.push_back(Lock());
              granule_to_cmd_queue_index[lock_id] = cmd_queue_idx++;
              CHECK(cmd_queue_idx == lock_buckets.size());
            }
          }
        }
      }
      //managed_granules_str = managed_replica_granules_str = "";
      LOG(INFO) << "Cluster worker id " << this_cluster_worker_id << " node worker id "<< worker_id
                << " partitions managed [" << managed_granules_str 
                << "], replica granules maanged [" << managed_replica_granules_str << "]" 
                << " is_replica_worker " << is_replica_worker << " replica_cluster_worker_id" << replica_cluster_worker_id;
      batch_per_worker = std::max(this->context.batch_size / this->context.worker_num, (std::size_t)1);
      this->context.hstore_active_active = batch_per_worker == 1;
      if (this->context.hstore_active_active) {
        LOG(INFO) << "HStore active active mode";
      }
      //lock_buckets.resize(this->context.partition_num * this->context.granules_per_partition);
      cluster_worker_messages.resize(cluster_worker_num);
      cluster_worker_messages_filled_in.resize(cluster_worker_num, false);
      for (int i = 0; i < (int)cluster_worker_num; ++i) {
        cluster_worker_messages[i] = std::make_unique<Message>();
        init_message(cluster_worker_messages[i].get(), i);
      }
      cluster_worker_messages.shrink_to_fit();
      this->message_stats.resize((size_t)HStoreMessage::NFIELDS, 0);
      this->message_sizes.resize((size_t)HStoreMessage::NFIELDS, 0);
      mp_concurrency_max = 2000;
  }

  ~HStoreExecutor() = default;


  void search(std::size_t table_id, std::size_t partition_id,
                  const void *key, void *value) const {
//    DCHECK((int)partition_owner_cluster_worker(partition_id) == this_cluster_worker_id);
    ITable *table = this->db.find_table(table_id, partition_id);
    auto value_bytes = table->value_size();
    auto row = table->search(key);
    HStoreHelper::read(row, value, value_bytes);
  }

  uint64_t generate_tid(TransactionType &txn) {
    static std::atomic<uint64_t> tid_counter{1};
    return tid_counter.fetch_add(1);
  }

  void abort(TransactionType &txn,
             std::vector<std::unique_ptr<Message>> &messages,
             bool write_cmd_buffer = false) {
    DCHECK(is_replica_worker == false);
    // assume all writes are updates
    if (!txn.is_single_partition()) {
      TxnCommand txn_cmd;

      for (auto i = 0u; i < txn.lock_status.num_locks(); ++i) {
        auto & lock = txn.lock_status.get_lock(i);
        if (lock.get_success() != TransactionType::LockStatus::SuccessState::SUCCEED)
          continue;
        if (lock.get_released())
          continue;
        auto lock_id = lock.get_lock_id();
        auto partition_id = lock_id_to_partition(lock_id);
        auto granule_id = lock_id_to_granule(lock_id);
        auto owner_cluster_worker = partition_owner_cluster_worker(partition_id, txn.ith_replica);

        if (owner_cluster_worker == this_cluster_worker_id) {
          auto & lock_bucket = get_lock_bucket(lock_id);
          if (lock.get_mode() == TransactionType::LockStatus::LockMode::READ) {
            
            if (is_replica_worker == false) {
              DCHECK(lock_bucket.reader_cnt() >= 1);
            }
            lock_bucket.dec_reader_cnt();
          } else {
            DCHECK(lock_bucket.write_locked());
            DCHECK(lock_bucket.get_last_writer() == txn.transaction_id);
            DCHECK(txn.transaction_id != lock.get_last_writer());
            lock_bucket.set_last_writer(lock.get_last_writer()); // Restore the last writer stored in lock
            lock_bucket.clear_write_lock();
          }
        } else {
          // send messages to other partitions to abort and unlock partitions
          // No need to wait for the response.
          //txn.pendingResponses++;
          auto tableId = 0;
          DCHECK(0 <= owner_cluster_worker && owner_cluster_worker < cluster_worker_num);
          auto table = this->db.find_table(tableId, partition_id);
          messages[owner_cluster_worker]->set_transaction_id(txn.transaction_id);
          
          txn_cmd.partition_id = partition_id;
          txn_cmd.granule_id = granule_id;
          txn_cmd.command_data = "";
          txn_cmd.tid = txn.transaction_id;
          txn_cmd.is_mp = true;
          bool write_lock = lock.get_mode() == TransactionType::LockStatus::LockMode::WRITE;
          DCHECK(0 <= owner_cluster_worker && owner_cluster_worker < cluster_worker_num);
          uint64_t requested_last_writer = lock.get_last_writer(); // Restore the last writer with last txn that sucessfully committed while write-locked the granule
          DCHECK(txn.transaction_id != requested_last_writer);
          DCHECK(write_cmd_buffer == false);
          txn.network_size += MessageFactoryType::new_release_lock_message(
              *messages[owner_cluster_worker], *table, this_cluster_worker_id, granule_id, requested_last_writer, write_lock, 
              false, txn.ith_replica, write_cmd_buffer, txn_cmd);
          //LOG(INFO) << "Abort release lock MP partition " << partition_id << " by cluster worker" << this_cluster_worker_id << " " << tid_to_string(txn.transaction_id) << " request sent";
          add_outgoing_message(owner_cluster_worker);
        }
        lock.set_released();
      }
      txn.message_flusher();
    } else {
      DCHECK(txn.pendingResponses == 0);
      DCHECK(txn.get_partition_count() == 1);
      for (auto i = 0u; i < txn.lock_status.num_locks(); ++i) {
        auto & lock = txn.lock_status.get_lock(i);
        if (lock.get_success() != TransactionType::LockStatus::SuccessState::SUCCEED)
          continue;
        if (lock.get_released())
          continue;
        auto lock_id = lock.get_lock_id();
        auto & lock_bucket = get_lock_bucket(lock_id);
        if (lock.get_mode() == TransactionType::LockStatus::LockMode::READ) {
          if (is_replica_worker == false) {
            DCHECK(lock_bucket.reader_cnt() >= 1);
          }
          lock_bucket.dec_reader_cnt();
        } else {
          DCHECK(lock_bucket.write_locked());
          DCHECK(lock_bucket.get_last_writer() == txn.transaction_id);
          DCHECK(txn.transaction_id != lock.get_last_writer());
          lock_bucket.set_last_writer(lock.get_last_writer()); // Restore the last writer with last txn that sucessfully committed while write-locked the granule
          lock_bucket.clear_write_lock();
        }
        lock.set_released();
      }
    }
    //txn.abort_lock_lock_released = true;
  }

  void write_command_for_sp_group(const std::vector<TransactionType*> & sp_txns) {
    ScopedTimer t0([&, this](uint64_t us) {
        serialize_cmd_time.add(us);
    });
    
    int64_t tid = -1;
    bool is_mp = false;
    int partition_id = -1;
    int granule_id = -1;
    bool is_coordinator = true;
    int64_t position_in_log = next_position_in_command_log++;
    minimum_coord_txn_written_log_position = position_in_log;

    size_t command_buffer_data_size = command_buffer_data.size();
    Encoder enc(command_buffer_data);
    enc << tid;
    enc << is_coordinator;
    enc << is_mp;
    enc << position_in_log;
    enc << partition_id;
    enc << granule_id;
    std::string txn_command_data;
    Encoder txn_command_data_encoder(txn_command_data);
    // First encode the bitmap
    auto bm_data = lock_bm.serialize();
    txn_command_data_encoder << bm_data.size();
    txn_command_data_encoder.write_n_bytes(bm_data.data(), bm_data.size());
    for (auto txn: sp_txns) {
      auto data = txn->serialize(1);
      txn_command_data_encoder << data.size();
      txn_command_data_encoder.write_n_bytes(data.data(), data.size());
      txn->command_written = true;
    }
    enc << txn_command_data.size();
    enc.write_n_bytes(txn_command_data.data(), txn_command_data.size());

    command_buffer_outgoing_data.insert(command_buffer_outgoing_data.end(), command_buffer_data.begin() + command_buffer_data_size, command_buffer_data.end());
    // Send to replica so that replica can start
    send_commands_to_replica(true);
  }

  void write_command(TransactionType &txn) {
    if (is_replica_worker == false && txn.command_written == false) {
      auto txn_command_data = txn.serialize(1);
      int64_t tid = txn.transaction_id;
      bool is_mp = true;//txn.is_single_partition() == false || txn.get_partition_granule_count(0) > 1;
      int partition_id = is_mp ? -1 : txn.get_partition(0);
      int granule_id = is_mp ? -1 : txn.get_granule(0, 0);
      bool is_coordinator = true;
      int64_t position_in_log = next_position_in_command_log++;
      uint64_t last_writer = 0;
      bool write_lock = false;
      bool is_cow_begin = false;
      bool is_cow_end = false;
      minimum_coord_txn_written_log_position = position_in_log;

      size_t command_buffer_data_size = command_buffer_data.size();
      Encoder enc(command_buffer_data);
      enc << tid;
      enc << is_coordinator;
      enc << is_mp;
      enc << position_in_log;
      enc << partition_id;
      enc << granule_id;
      enc << last_writer;
      enc << write_lock;
      enc << is_cow_begin;
      enc << is_cow_end;
      enc << txn_command_data.size();
      enc.write_n_bytes(txn_command_data.data(), txn_command_data.size());
      
      if (this->replica_num > 1) {
        command_buffer_outgoing_data.insert(command_buffer_outgoing_data.end(), command_buffer_data.begin() + command_buffer_data_size, command_buffer_data.end());
      }
      
      //command_buffer.push_back(cmd);
      //command_buffer_outgoing.push_back(cmd);
      txn.command_written = true;
    }
  }


  bool commit_transaction(TransactionType &txn,
              std::vector<std::unique_ptr<Message>> &messages) {
    // if (is_replica_worker) { // Should always succeed for replica
    //   CHECK(txn.abort_lock == false);
    // }
    if (txn.abort_lock) {
      //DCHECK(txn.is_single_partition() == false);
      if (is_replica_worker == false) {
        // We only release locks when executing on active replica
        abort(txn, messages);
      }
      
      return false;
    }
    DCHECK(this->context.hstore_command_logging == true);

    uint64_t commit_tid = generate_tid(txn);
    DCHECK(txn.get_logger());

    write_command(txn);

    {
      ScopedTimer t([&, this](uint64_t us) {
        txn.record_commit_write_back_time(us);
      });
      write_back_command_logging(txn, commit_tid, messages);
    }
    {
      ScopedTimer t([&, this](uint64_t us) {
        txn.record_commit_unlock_time(us);
      });
      if (this->context.hstore_active_active && is_replica_worker == false) {
        send_participant_command(txn, messages);
      } if (is_replica_worker || (this->context.hstore_active_active == false && txn.is_single_partition())) {
        release_locks_async(txn, messages, true);
      } else {
        txn.message_flusher();
      }
    }

    return true;
  }

  void send_participant_command(TransactionType &txn, std::vector<std::unique_ptr<Message>> &messages) {
    DCHECK(is_replica_worker == false);
    if (txn.is_single_partition())
      return;
    
    TxnCommand txn_cmd;

    for (auto i = 0u; i < txn.lock_status.num_locks(); ++i) {
      auto & lock = txn.lock_status.get_lock(i);
      if (lock.get_success() != TransactionType::LockStatus::SuccessState::SUCCEED)
        continue;
      DCHECK(lock.get_released() == false);
      auto lock_id = lock.get_lock_id();
      auto partition_id = lock_id_to_partition(lock_id);
      auto granule_id = lock_id_to_granule(lock_id);
      auto owner_cluster_worker = partition_owner_cluster_worker(partition_id, txn.ith_replica);

      if (owner_cluster_worker != this_cluster_worker_id) {
        auto tableId = 0;
        auto table = this->db.find_table(tableId, partition_id);
        txn_cmd.partition_id = partition_id;
        txn_cmd.granule_id = granule_id;
        txn_cmd.command_data = "";
        txn_cmd.tid = txn.transaction_id;
        txn_cmd.is_mp = true;
        bool write_lock = lock.get_mode() == TransactionType::LockStatus::LockMode::WRITE;
        // send messages to unlock partitions;
        messages[owner_cluster_worker]->set_transaction_id(txn.transaction_id);
        DCHECK(0 <= owner_cluster_worker && owner_cluster_worker < cluster_worker_num);
        uint64_t requested_last_writer = txn.transaction_id;
        if (write_lock) {
          requested_last_writer = txn.transaction_id;
          DCHECK(requested_last_writer != lock.get_last_writer());
        } else {
          requested_last_writer = lock.get_last_writer();
        }
        txn.network_size += MessageFactoryType::new_write_participant_command_message(
            *messages[owner_cluster_worker], *table, this_cluster_worker_id, granule_id, requested_last_writer, write_lock, 
            txn.ith_replica, txn_cmd);
        add_outgoing_message(owner_cluster_worker);
      }
    }

    txn.message_flusher();
  }

  void release_locks_async(TransactionType &txn, std::vector<std::unique_ptr<Message>> &messages, bool write_cmd_buffer, bool flush_message = true) {
    if (is_replica_worker) {
      DCHECK(txn.ith_replica != 0);
      DCHECK(write_cmd_buffer == false);
    }
    txn.release_lock_called = true;
    if (txn.is_single_partition() == false) {
      TxnCommand txn_cmd;

      for (auto i = 0u; i < txn.lock_status.num_locks(); ++i) {
        auto & lock = txn.lock_status.get_lock(i);
        if (lock.get_success() != TransactionType::LockStatus::SuccessState::SUCCEED)
          continue;
        if (lock.get_released())
          continue;
        auto lock_id = lock.get_lock_id();
        auto partition_id = lock_id_to_partition(lock_id);
        auto granule_id = lock_id_to_granule(lock_id);
        auto owner_cluster_worker = partition_owner_cluster_worker(partition_id, txn.ith_replica);

        if (owner_cluster_worker == this_cluster_worker_id) {
          auto & lock_bucket = get_lock_bucket(lock_id);
          if (lock.get_mode() == TransactionType::LockStatus::LockMode::READ) {
            if (is_replica_worker == false) {
              DCHECK(lock_bucket.reader_cnt() >= 1);
            }
            DCHECK(lock_bucket.get_last_writer() == lock.get_last_writer());
            lock_bucket.dec_reader_cnt();
          } else {
            DCHECK(lock_bucket.write_locked());
            DCHECK(lock_bucket.get_last_writer() == txn.transaction_id);
            DCHECK(txn.transaction_id != lock.get_last_writer());
            lock_bucket.clear_write_lock();
          }
          if (is_replica_worker) {
            auto & q = get_granule_lock_cmd_queue(lock_id);
            if (q.empty() == false) {
              DCHECK(granule_command_queue_processing[granule_to_cmd_queue_index[lock_id]] == false);
              replay_commands_in_granule(lock_id);
            }
            auto & q2 = get_granule_lock_request_queue(lock_id);
            if (q2.empty() == false) {
              granule_lock_reqeust_candidates.push_back(lock_id);
            }
          }
        } else {
          auto tableId = 0;
          auto table = this->db.find_table(tableId, partition_id);
          txn_cmd.partition_id = partition_id;
          txn_cmd.granule_id = granule_id;
          txn_cmd.command_data = "";
          txn_cmd.tid = txn.transaction_id;
          txn_cmd.is_mp = true;
          bool write_lock = lock.get_mode() == TransactionType::LockStatus::LockMode::WRITE;
          // send messages to unlock partitions;
          messages[owner_cluster_worker]->set_transaction_id(txn.transaction_id);
          DCHECK(0 <= owner_cluster_worker && owner_cluster_worker < cluster_worker_num);
          uint64_t requested_last_writer = txn.transaction_id;
          if (write_lock) {
            requested_last_writer = txn.transaction_id;
            DCHECK(requested_last_writer != lock.get_last_writer());
          } else {
            requested_last_writer = lock.get_last_writer();
          }
          txn.network_size += MessageFactoryType::new_release_lock_message(
              *messages[owner_cluster_worker], *table, this_cluster_worker_id, granule_id, requested_last_writer, write_lock, 
              false, txn.ith_replica, write_cmd_buffer, txn_cmd);
          add_outgoing_message(owner_cluster_worker);
        }
        lock.set_released();
      }

      if (flush_message) {
        txn.message_flusher();
      }
    } else {
      DCHECK(txn.get_partition_count() == 1);

      for (auto i = 0u; i < txn.lock_status.num_locks(); ++i) {
        auto & lock = txn.lock_status.get_lock(i);
        if (lock.get_success() != TransactionType::LockStatus::SuccessState::SUCCEED)
          continue;
        if (lock.get_released())
          continue;
        auto lock_id = lock.get_lock_id();

        auto & lock_bucket = get_lock_bucket(lock_id);
        if (lock.get_mode() == TransactionType::LockStatus::LockMode::READ) {
          if (is_replica_worker == false) {
            DCHECK(lock_bucket.reader_cnt() >= 1);
          }
          DCHECK(lock_bucket.get_last_writer() == lock.get_last_writer());
          lock_bucket.dec_reader_cnt();
        } else {
          DCHECK(lock_bucket.write_locked());
          DCHECK(lock_bucket.get_last_writer() == txn.transaction_id);
          DCHECK(txn.transaction_id != lock.get_last_writer());
          lock_bucket.clear_write_lock();
        }
        lock.set_released();

        if (is_replica_worker) {
          auto & q = get_granule_lock_cmd_queue(lock_id);
          if (q.empty() == false) {
            DCHECK(granule_command_queue_processing[granule_to_cmd_queue_index[lock_id]] == false);
            replay_commands_in_granule(lock_id);
          }
          auto & q2 = get_granule_lock_request_queue(lock_id);
          if (q2.empty() == false) {
            granule_lock_reqeust_candidates.push_back(lock_id);
          }
        }
      }
    }
  }

  std::vector<char> undo_buffer;
  void write_back_command_logging(TransactionType &txn, uint64_t commit_tid,
                  std::vector<std::unique_ptr<Message>> &messages) {
    //auto &readSet = txn.readSet;
    auto &writeSet = txn.writeSet;
    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto owner_cluster_worker = partition_owner_cluster_worker(partitionId, txn.ith_replica);
      auto table = this->db.find_table(tableId, partitionId);

      // write
      if ((int)owner_cluster_worker == this_cluster_worker_id) {
        auto key = writeKey.get_key();
        auto value = writeKey.get_value();
        if (txn.is_single_partition() == false || is_replica_worker) {
          table->update(key, value);
        } else {
          auto key_size = table->key_size();
          auto value_size = table->value_size();
          //table->update(key, value);
          table->update(key, value, [&, this](const void * key, const void * value) {
            auto old_size = this->undo_buffer.size();
            this->undo_buffer.resize(old_size + key_size);
            memcpy(this->undo_buffer.data() + old_size, key, key_size);
            old_size = this->undo_buffer.size();
            this->undo_buffer.resize(old_size + value_size);
            memcpy(this->undo_buffer.data() + old_size, value, value_size);
          });
        }
      } else {
        //txn.pendingResponses++;
        DCHECK(0 <= owner_cluster_worker && owner_cluster_worker < cluster_worker_num);
        messages[owner_cluster_worker]->set_transaction_id(txn.transaction_id);
        txn.network_size += MessageFactoryType::new_write_back_message(
            *messages[owner_cluster_worker], *table, writeKey.get_key(),
            writeKey.get_value(), this_cluster_worker_id, writeKey.get_granule_id(), commit_tid, txn.ith_replica, false);
        //LOG(INFO) << "Partition worker " << this_cluster_worker_id << " issueed write request on partition " << partitionId;
        add_outgoing_message(owner_cluster_worker);
      }
    }
    // if (txn.is_single_partition() == false) {
    //   sync_messages(txn, true);
    // }
  }

  void release_lock(TransactionType &txn, uint64_t commit_tid,
                    std::vector<std::unique_ptr<Message>> &messages) {
    if (txn.is_single_partition()) {
      // For single-partition transactions, do nothing.
      // release single partition lock

    } else {

      sync_messages(txn);
    }

  }

  void sync_messages(TransactionType &txn, bool wait_response = true) {              
    txn.message_flusher();
    if (wait_response) {
      //LOG(INFO) << "Waiting for " << txn.pendingResponses << " responses";
      while (txn.pendingResponses > 0) {
        txn.remote_request_handler(0);
      }
    }
  }

  void setupHandlers(TransactionType &txn)

      override {
    txn.lock_request_handler =
        [this, &txn](std::size_t table_id, std::size_t partition_id, std::size_t granule_id,
                     uint32_t key_offset, const void *key, void *value,
                     bool local_index_read, bool write_lock, bool &success,
                     bool &remote) {
      auto transaction_id = txn.transaction_id;
      if (is_replica_worker == true && sp_parallel_exec_commit && txn.is_single_partition() && txn.replay_bm_for_sp->get_bit(to_lock_id(partition_id, granule_id))) {
        success = false;
        remote = false;
        txn.abort_lock_bm = true;
        txn.finished_commit_phase = true;
        return;
      }
      int owner_cluster_worker = partition_owner_cluster_worker(partition_id, txn.ith_replica);
      if (local_index_read) {
        remote = false;
        success = true;
        this->search(table_id, partition_id, key, value);
        return;
      }

      auto lock_index = txn.readSet[key_offset].get_lock_index();
      DCHECK(lock_index != -1);
      auto success_state = txn.lock_status.get_lock(lock_index).get_success() ;

      if ((int)owner_cluster_worker == this_cluster_worker_id) {
        remote = false;
        auto lock_id = to_lock_id(partition_id, granule_id);
        auto & lock_bucket = get_lock_bucket(lock_id);
        // TODO: replay lock_buckets
        // if (is_replica_worker && lock_bucket != txn.transaction_id) {
        //   success = false;
        //   return;
        // }

        // if (lock_bucket != -1 && lock_bucket != txn.transaction_id) {
        //   success = false;
        //   return;
        // }
        if (is_replica_worker) {
          if (write_lock) {
            DCHECK(lock_bucket.get_last_writer() == txn.transaction_id);
            DCHECK(lock_bucket.write_locked());
          } else {
            DCHECK(lock_bucket.get_last_writer() == txn.lock_status.get_lock(lock_index).get_last_writer());
            if (is_replica_worker == false) {
              DCHECK(lock_bucket.reader_cnt() > 0);
            }
          }
        } else {
          if (success_state == TransactionType::LockStatus::SuccessState::INIT) { // fisrt lock attempt of this transaction on this granule
            auto lock_bucket_last_writer = lock_bucket.get_last_writer();
            if (write_lock) {
              DCHECK(lock_bucket.reader_cnt() >= 0);
              if (lock_bucket.reader_cnt() > 0) {
                success = false;
                return;
              } else if (lock_bucket.write_locked() == false) { // No reader and not write locked
                txn.lock_status.get_lock(lock_index).set_last_writer(lock_bucket_last_writer);
                lock_bucket.set_last_writer(txn.transaction_id);
                lock_bucket.set_write_lock();
              } else if (lock_bucket.get_last_writer() != txn.transaction_id){ // Write locked by others
                success = false; 
                return;
              } else {
                // Locked by `txn.transaction_id`
              }
            } else {
              // Read lock
              if (lock_bucket.write_locked()) { // write-locked
                success = false;
                return;
              } else {
                lock_bucket.inc_reader_cnt();
                txn.lock_status.get_lock(lock_index).set_last_writer(lock_bucket_last_writer);
              }
            }
          } else if (success_state == TransactionType::LockStatus::SuccessState::FAILED) {
            success = false;
            return;
          }
        }

        // if (lock_bucket == -1 && is_replica_worker)
        //    LOG(INFO) << "Tranasction from worker " << this_cluster_worker_id << " locked partition " << lock_id << " txn " << tid_to_string(txn.transaction_id);;
        // if (lock_bucket == -1) {
        //   DCHECK(lock_bm.get_bit(lock_id) == false);
        //   lock_bucket = txn.transaction_id;
        //   lock_bm.set_bit(lock_id);
        //   DCHECK(lock_bm.get_bit(lock_id) == true);
        // }

        success = true;

        this->search(table_id, partition_id, key, value);

      } else {
        // bool in_parts = false;
        // for (auto i = 0; i < txn.get_partition_count(); ++i) {
        //   if ((int)partition_id == txn.get_partition(i)) {
        //     in_parts = true;
        //     break;
        //   }
        // }
        // DCHECK(in_parts);
        ITable *table = this->db.find_table(table_id, partition_id);

        remote = true;

        // LOG(INFO) << "Requesting locking partition " << partition_id << " by cluster worker" << this_cluster_worker_id << " on owner_cluster_worker " << owner_cluster_worker; 
        DCHECK(0 <= owner_cluster_worker && owner_cluster_worker < cluster_worker_num);
        cluster_worker_messages[owner_cluster_worker]->set_transaction_id(txn.transaction_id);
        DCHECK(success_state != TransactionType::LockStatus::SuccessState::FAILED);
        DCHECK(success_state != TransactionType::LockStatus::SuccessState::SUCCEED);
        bool request_lock = success_state == TransactionType::LockStatus::SuccessState::INIT;
        uint64_t granule_last_writer = txn.lock_status.get_lock(lock_index).get_last_writer();
        if (is_replica_worker && write_lock) {
          DCHECK(granule_last_writer != std::numeric_limits<uint64_t>::max());
          granule_last_writer = txn.transaction_id; // for write lock request, the last successful writer is the transaction itself.
        }
        txn.network_size += MessageFactoryType::new_acquire_lock_and_read_message(
              *(cluster_worker_messages[owner_cluster_worker]), *table, key, key_offset, this_cluster_worker_id, 
              granule_id, granule_last_writer, write_lock, request_lock, txn.ith_replica, 0);
        add_outgoing_message(owner_cluster_worker);
        txn.distributed_transaction = true;
        txn.pendingResponses++;
      }
    };

    txn.remote_request_handler = [this](std::size_t) { return this->handle_requests(); };
    txn.message_flusher = [this]() { this->flush_messages(); };
    txn.get_table = [this](std::size_t tableId, std::size_t partitionId) { return this->db.find_table(tableId, partitionId); };
    txn.set_logger(this->logger);
  };

  using Transaction = TransactionType;

  int cnt = 0;
  bool acquire_lock_and_read_request_handler(const Message & inputMessage, MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    ++cnt;
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::ACQUIRE_LOCK_AND_READ_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();
    uint64_t tid = inputMessage.get_transaction_id();

    /*
     * The structure of a write lock request: (primary key, key offset, request_remote_worker_id, ith_replica)
     * The structure of a write lock response: (success?, key offset, value?)
     */
    uint32_t request_remote_worker_id;
    uint32_t granule_id;
    auto stringPiece = inputPiece.toStringPiece();
    uint32_t key_offset;
    uint64_t requested_last_writer;
    bool write_lock = false;
    bool request_lock = false;
    std::size_t ith_replica;

    DCHECK(inputPiece.get_message_length() ==
           MessagePiece::get_header_size() + key_size + sizeof(key_offset) + sizeof(requested_last_writer) + sizeof(write_lock) + sizeof(request_lock) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(std::size_t) + sizeof(uint64_t));

    const void *key = stringPiece.data();
    //uint64_t ts3 = std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()).time_since_epoch().count();
    uint64_t ts3 = 0;
    uint64_t ts1;
    uint64_t lock_bucket_last_writer = 0;
    stringPiece.remove_prefix(key_size);
    star::Decoder dec(stringPiece);
    dec >> key_offset >> request_remote_worker_id >> granule_id >> requested_last_writer >> write_lock >> request_lock >> ith_replica >> ts1;

    DCHECK(granule_id == inputPiece.get_granule_id());
    if (ith_replica > 0)
      DCHECK(is_replica_worker);

    DCHECK((int)partition_owner_cluster_worker(partition_id, ith_replica) == this_cluster_worker_id);

    DCHECK(dec.size() == 0);
    char success = 0;
    auto lock_id = to_lock_id(partition_id, granule_id);
    auto & lock_bucket = get_lock_bucket(lock_id);
    lock_bucket_last_writer = lock_bucket.get_last_writer();
    if (is_replica_worker) {
      success = 1;
      // TODO: replay lock_buckets
      replay_commands_in_granule(lock_id);
      if (write_lock) {
        if (lock_bucket.write_locked() == false || lock_bucket.get_last_writer() != requested_last_writer) {
          success = 0;
        }
      } else {
        if (lock_bucket.write_locked() ||
            //lock_bucket.reader_cnt() == 0 || 
            lock_bucket.get_last_writer() != requested_last_writer) {
          success = 0;
        }
      }
      if (success != 1) {
        auto & q = get_granule_lock_cmd_queue(lock_id);
        int distance = 0;
        for (std::size_t i = 0; i < q.size(); ++i) {
          if (q[i].tid == tid) {
            distance = i + 1;
            break;
          }
        }
        if (distance != 0) {
          //window_lock_req_latency.add(distance);
          //this->last_window_lock_req_latency.store(window_lock_req_latency.average());
          //distance = 100;
          this->n_failed_cmd_not_ready++;
        } else {
          this->n_failed_no_cmd++;
        }
        if (write_lock) {
          this->n_failed_write_lock++;
        } else {
          this->n_failed_read_lock++;
        }
        return false;
      }
    } else {
  //     if (lock_bucket == -1 || lock_bucket == tid) {
  //       //lock it;
  //       // if (lock_bucket == -1)
  //       //    LOG(INFO) << "Partition " << lock_id << " locked and read by remote cluster worker " << request_remote_worker_id << " by this_cluster_worker_id " << this_cluster_worker_id << " ith_replica "  << ith_replica << " txn " << tid_to_string(tid);
  //       lock_bucket = tid;
  //       lock_bm.set_bit(lock_id);
  //       success = 1;
  //     } else {
  //       //  LOG(INFO) << "Partition " << lock_id << " was failed to be locked by cluster worker " << request_remote_worker_id << " and txn " << tid_to_string(tid)
  // //                 << " already locked by " << tid_to_string(lock_bucket) << " ith_replica" << ith_replica;
  //     }
      if (request_lock) {
        if (write_lock) {
          DCHECK(lock_bucket.reader_cnt() >= 0);
          if (lock_bucket.reader_cnt() > 0) {
            success = 0;
          } else if (lock_bucket.write_locked() == false) { // No reader and not write locked
            lock_bucket.set_last_writer(tid);
            lock_bucket.set_write_lock();
            success = 1;
          } else if (lock_bucket.get_last_writer() != tid) { // Write locked by others
            success = 0;
          } else { // Locked by `tid`
            success = 1;
          }
        } else {
          // Read lock
          if (lock_bucket.write_locked() == false) { // write-locked
            lock_bucket.inc_reader_cnt();
            success = 1;
          }
        }
      } else {
        success = 1;
      }
    }
    // prepare response message header
    auto message_size =
        MessagePiece::get_header_size() + sizeof(success) + sizeof(key_offset) + sizeof(lock_bucket_last_writer) + 
        sizeof(write_lock) + sizeof(request_lock) + sizeof(uint64_t) * 3;

    if (success == 1) {
      //window_lock_req_latency.add(std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()).time_since_epoch().count() - inputMessage.get_message_recv_time());
      //this->last_window_lock_req_latency.store(window_lock_req_latency.average());
      message_size += value_size;
    } else{
      // LOG(INFO) << "acquire_partition_lock_request from cluster worker " << request_remote_worker_id
      //            << " on partition " << lock_id
      //            << " partition locked acquired faliled, lock owned by " << lock_bucket;
    }
    
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::ACQUIRE_LOCK_AND_READ_RESPONSE), message_size,
        table_id, partition_id, granule_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    encoder << success << key_offset << lock_bucket_last_writer << write_lock << request_lock << ts1 << ts3;

    if (success == 1) {
      auto row = table.search(key);
      //uint64_t ts2 = std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()).time_since_epoch().count();
      uint64_t ts2 = 0;
      encoder << ts2;
      // reserve size for read
      responseMessage.data.append(value_size, 0);
      void *dest =
          &responseMessage.data[0] + responseMessage.data.size() - value_size;
      // read to message buffer
      HStoreHelper::read(row, dest, value_size);
    } else {
      uint64_t ts2 = 0;
      encoder << ts2;
    }

    responseMessage.set_is_replica(ith_replica > 0);
    responseMessage.set_transaction_id(inputMessage.get_transaction_id());
    responseMessage.flush();
    responseMessage.set_gen_time(Time::now());
    return success == 1;
  }
  std::deque<std::pair<int64_t,int>> replay_points;
  struct ReplayBuffer {
    std::string data;
    Decoder * dec = nullptr;
    ReplayBuffer(const std::string & buf): data(buf), dec(new Decoder(data)) {}
    ReplayBuffer(const std::string && buf): data(std::move(buf)), dec(new Decoder(data)) {}
    ~ReplayBuffer() { delete dec; }
  };

  std::deque<ReplayBuffer*> replay_buffers;

// TODO: replay lock_buckets
  int checkpoint_step = 0;

  void decode_replay_buffer() {
    if (replay_buffers.empty())
      return;
    auto enqueue_mp_transaction = [this](TxnCommandBase & cmd, TransactionType * mp_txn){
      mp_txn->position_in_log = cmd.position_in_log;
      auto partition_count = mp_txn->get_partition_count();
      cmd.txn = mp_txn;
      DCHECK(mp_txn->ith_replica > 0);

      bool mp_queued = false;
      int first_lock_id_by_this_worker = -1;
      int granule_locks = 0;
      for (int32_t j = 0; j < partition_count; ++j) {
        auto partition_id = mp_txn->get_partition(j);
        auto partition_responsible_worker = partition_owner_cluster_worker(partition_id, 1);
        auto partition_granule_count = mp_txn->get_partition_granule_count(j);
        if (partition_responsible_worker == this_cluster_worker_id) {
          granule_locks += partition_granule_count;
        }
      }
      mp_txn->granules_left_to_lock = granule_locks;
      for (int32_t j = 0; j < partition_count; ++j) {
        auto partition_id = mp_txn->get_partition(j);
        auto partition_responsible_worker = partition_owner_cluster_worker(partition_id, 1);
        auto partition_granule_count = mp_txn->get_partition_granule_count(j);
        if (partition_responsible_worker == this_cluster_worker_id) {
          for (int k = 0; k < partition_granule_count; ++k) {
            auto granule_id = mp_txn->get_granule(j, k);
            DCHECK(granule_id >= 0 && granule_id < (int)this->context.granules_per_partition);
            auto lock_id = to_lock_id(partition_id, granule_id);
            first_lock_id_by_this_worker = lock_id;
            auto & q = get_granule_lock_cmd_queue(lock_id);
            TxnCommandBase cmd_mp;
            cmd_mp.tid = cmd.tid;
            cmd_mp.is_coordinator = false;
            cmd_mp.partition_id = partition_id;
            cmd_mp.granule_id = granule_id;
            cmd_mp.position_in_log = cmd.position_in_log;
            cmd_mp.is_mp = true;
            cmd_mp.txn = (void*)mp_txn;
            cmd_mp.is_cow_begin = false;
            cmd_mp.is_cow_end = false;
            DCHECK(mp_txn->lock_status.num_locks() > 0);
            auto lock_index = mp_txn->lock_status.get_lock_index_no_write(lock_id);
            DCHECK(lock_index != -1);
            cmd_mp.last_writer = mp_txn->lock_status.get_lock(lock_index).get_last_writer();
            DCHECK(cmd_mp.last_writer != std::numeric_limits<uint64_t>::max());
            cmd_mp.write_lock = mp_txn->lock_status.get_lock(lock_index).get_mode() == TransactionType::LockStatus::LockMode::WRITE;

            auto & lock_bucket = get_lock_bucket(lock_id);

            if (cmd_mp.write_lock) {
              if (lock_bucket.free()) { // only acquire the lock when there is no readers and writer
                lock_bucket.set_last_writer(cmd_mp.tid);
                lock_bucket.set_write_lock();
                if (cmd_mp.txn != nullptr && --((TransactionType*)cmd_mp.txn)->granules_left_to_lock == 0) {
                  txns_candidate_for_reexecution.push_back((TransactionType*)cmd_mp.txn);
                }
                DCHECK(get_partition_last_replayed_position_in_log(lock_id) <= cmd_mp.position_in_log);
                get_partition_last_replayed_position_in_log(lock_id) = cmd_mp.position_in_log;
              } else {
                q.emplace_back(std::move(cmd_mp));
              }
            } else {
              if (lock_bucket.write_locked() || lock_bucket.get_last_writer() != cmd_mp.last_writer) {
                q.emplace_back(std::move(cmd_mp));
              } else {
                DCHECK(lock_bucket.get_last_writer() == cmd_mp.last_writer);
                lock_bucket.inc_reader_cnt();
                if (cmd_mp.txn != nullptr && --((TransactionType*)cmd_mp.txn)->granules_left_to_lock == 0) {
                  txns_candidate_for_reexecution.push_back((TransactionType*)cmd_mp.txn);
                }
                //LOG(INFO) << "This cluster worker " << this_cluster_worker_id << " replayed participant read lock record of txn " << cmd.tid << " on lock_id " << lock_id << " with last writer " << cmd.last_writer;
                DCHECK(get_partition_last_replayed_position_in_log(lock_id) <= cmd_mp.position_in_log);
                get_partition_last_replayed_position_in_log(lock_id) = cmd_mp.position_in_log;
              }
            }

            auto & q2 = get_granule_lock_request_queue(lock_id);
            if (q2.empty() == false) {
              granule_lock_reqeust_candidates.push_back(lock_id);
            }
          }
        }
      }
      DCHECK(first_lock_id_by_this_worker != -1);
      mp_txn->replay_queue_lock_id = first_lock_id_by_this_worker;
    };

    if (replay_buffers.front()->dec->size() == 0) {
      delete replay_buffers.front();
      replay_buffers.pop_front();
    }
    if (replay_buffers.empty())
      return;
    int cnt = 10; // decode at most 10 entries at a time
    auto & dec = *(replay_buffers.front()->dec);
    int64_t max_posisiton_in_log_this_batch = 0;
    int mp_count_this_batch = 0;
    while (cnt-- && dec.size() > 0) {
      TxnCommandBase cmd;
      dec >> cmd.tid;
      dec >> cmd.is_coordinator;
      dec >> cmd.is_mp;
      dec >> cmd.position_in_log;
      dec >> cmd.partition_id;
      dec >> cmd.granule_id;
      dec >> cmd.last_writer;
      dec >> cmd.write_lock;
      dec >> cmd.is_cow_begin;
      dec >> cmd.is_cow_end;
      max_posisiton_in_log_this_batch = std::max(max_posisiton_in_log_this_batch, cmd.position_in_log);
      std::size_t command_data_size;
      dec >> command_data_size;
      std::string command_data = std::string(dec.get_raw_ptr(), command_data_size);
      dec.remove_prefix(command_data_size);
      if (cmd.is_cow_begin || cmd.is_cow_end) {
        std::string tag = cmd.is_cow_begin ? "COW_BEGIN" : "COW_END";
        
        ReplicaCheckpointMeta *ckpt_meta = new ReplicaCheckpointMeta{cmd.is_cow_begin, cmd.is_cow_end, 0};
        ckpt_meta->checkpoint_step = ++checkpoint_step;
        LOG(INFO) << "Replica worker " << this_cluster_worker_id << " found a " << tag << " at step " << ckpt_meta->checkpoint_step;
        for (auto lock_id : lock_ids_by_this_replica_executor) {
          auto lock_index = granule_to_cmd_queue_index[lock_id];
          auto & q = granule_command_queues[lock_index];
          auto cmd_copy = cmd;
          cmd.partition_id = lock_id_to_partition(lock_id);
          cmd.granule_id = lock_id_to_granule(lock_id);
          cmd_copy.txn = (void*)ckpt_meta;
          ckpt_meta->ref_cnt++;
          q.push_back(cmd_copy);
        }

        for (auto lock_id : lock_ids_by_this_replica_executor) {
          DCHECK(granule_command_queue_processing[granule_to_cmd_queue_index[lock_id]] == false);
          replay_commands_in_granule(lock_id);
          auto & q2 = get_granule_lock_request_queue(lock_id);
          if (q2.empty() == false) {
            granule_lock_reqeust_candidates.push_back(lock_id);
          }
        }
      } else if (!cmd.is_coordinator) { // place into one partition command queue for replay
        auto partition = cmd.partition_id;
        auto granule_id = cmd.granule_id;
        DCHECK(granule_id >= 0 && granule_id < (int)this->context.granules_per_partition);
        auto lock_id = to_lock_id(partition, granule_id);
        DCHECK(partition != -1);
        auto lock_index = granule_to_cmd_queue_index[lock_id];
        auto & q = granule_command_queues[lock_index];
        cmd.txn = nullptr;

        auto & lock_bucket = get_lock_bucket(lock_id);
        
        if (cmd.write_lock) {
          if (lock_bucket.free()) { // only acquire the lock when there is no readers and writer
            lock_bucket.set_last_writer(cmd.tid);
            lock_bucket.set_write_lock();
            DCHECK(get_partition_last_replayed_position_in_log(lock_id) <= cmd.position_in_log);
            get_partition_last_replayed_position_in_log(lock_id) = cmd.position_in_log;
          } else {
            q.emplace_back(std::move(cmd));
          }
        } else {
          if (lock_bucket.write_locked() || lock_bucket.get_last_writer() != cmd.last_writer) {
            q.emplace_back(std::move(cmd));
          } else {
            DCHECK(lock_bucket.get_last_writer() == cmd.last_writer);
            lock_bucket.inc_reader_cnt();
            //LOG(INFO) << "This cluster worker " << this_cluster_worker_id << " replayed participant read lock record of txn " << cmd.tid << " on lock_id " << lock_id << " with last writer " << cmd.last_writer;
            DCHECK(get_partition_last_replayed_position_in_log(lock_id) <= cmd.position_in_log);
            get_partition_last_replayed_position_in_log(lock_id) = cmd.position_in_log;
          }
        }

        // if (lock_bucket.write_locked() == false) {
        //   DCHECK(granule_command_queue_processing[granule_to_cmd_queue_index[lock_id]] == false);
        //   replay_commands_in_granule(lock_id);
        // }
          
        auto & q2 = get_granule_lock_request_queue(lock_id);
        if (q2.empty() == false) {
          granule_lock_reqeust_candidates.push_back(lock_id);
        }
      } else if (cmd.is_mp == false) { // place into one partition command queue for replay
        CHECK(cmd.granule_id == -1);
        CHECK(cmd.partition_id == -1);
        CHECK(false);
        Decoder dec_sp_data(command_data);
        uint64_t bitmap_size;
        dec_sp_data >> bitmap_size;
        std::string bitmap_raw(dec_sp_data.get_raw_ptr(), bitmap_size);
        dec_sp_data.remove_prefix(bitmap_size);
        lock_bitmap * bm = lock_bitmap::deserialize_from_raw(bitmap_raw);
        while (dec_sp_data.size()) {
          uint64_t command_data_size;
          dec_sp_data >> command_data_size;
          std::string command_data(dec_sp_data.get_raw_ptr(), command_data_size);
          dec_sp_data.remove_prefix(command_data_size);
          auto mp_txn = this->workload.deserialize_from_raw(this->context, command_data).release();
          mp_txn->context = &this->context;
          CHECK(mp_txn->is_single_partition());
          bm->inc_ref();
          mp_txn->replay_bm_for_sp = bm;
          TxnCommandBase cmd_mp_txn = cmd;
          cmd_mp_txn.tid = mp_txn->transaction_id;
          cmd_mp_txn.is_coordinator = true;
          cmd_mp_txn.txn = (void*)mp_txn;
          cmd_mp_txn.is_mp = true;
          enqueue_mp_transaction(cmd_mp_txn, mp_txn);
        }
      } else { // place into multiple partition command queues for replay
        DCHECK(cmd.partition_id == -1);
        auto mp_txn = this->workload.deserialize_from_raw(this->context, command_data).release();
        mp_txn->startTime = std::chrono::steady_clock::now();
        mp_txn->context = &this->context;
        enqueue_mp_transaction(cmd, mp_txn);
        mp_count_this_batch++;
      }
    }
    if (mp_count_this_batch > 0) {
      //replay_points.push_back(std::make_pair(max_posisiton_in_log_this_batch, mp_count_this_batch));
    }
  }
  void spread_replicated_commands(const std::string & buffer) {
    DCHECK(is_replica_worker);
    Decoder dec(buffer);
    std::size_t cmd_cnt = 0;
    int64_t max_posisiton_in_log_this_batch = 0;
    int mp_count_this_batch = 0;
    auto enqueue_mp_transaction = [this](TxnCommandBase & cmd, TransactionType * mp_txn){
      mp_txn->position_in_log = cmd.position_in_log;
      auto partition_count = mp_txn->get_partition_count();
      cmd.txn = mp_txn;
      DCHECK(mp_txn->ith_replica > 0);

      bool mp_queued = false;
      int first_lock_id_by_this_worker = -1;
      int granule_locks = 0;
      for (int32_t j = 0; j < partition_count; ++j) {
        auto partition_id = mp_txn->get_partition(j);
        auto partition_responsible_worker = partition_owner_cluster_worker(partition_id, 1);
        auto partition_granule_count = mp_txn->get_partition_granule_count(j);
        if (partition_responsible_worker == this_cluster_worker_id) {
          granule_locks += partition_granule_count;
        }
      }
      mp_txn->granules_left_to_lock = granule_locks;
      for (int32_t j = 0; j < partition_count; ++j) {
        auto partition_id = mp_txn->get_partition(j);
        auto partition_responsible_worker = partition_owner_cluster_worker(partition_id, 1);
        auto partition_granule_count = mp_txn->get_partition_granule_count(j);
        if (partition_responsible_worker == this_cluster_worker_id) {
          for (int k = 0; k < partition_granule_count; ++k) {
            auto granule_id = mp_txn->get_granule(j, k);
            DCHECK(granule_id >= 0 && granule_id < (int)this->context.granules_per_partition);
            auto lock_id = to_lock_id(partition_id, granule_id);
            first_lock_id_by_this_worker = lock_id;
            auto & q = get_granule_lock_cmd_queue(lock_id);
            TxnCommandBase cmd_mp;
            cmd_mp.tid = cmd.tid;
            cmd_mp.is_coordinator = false;
            cmd_mp.partition_id = partition_id;
            cmd_mp.granule_id = granule_id;
            cmd_mp.position_in_log = cmd.position_in_log;
            cmd_mp.is_mp = true;
            cmd_mp.txn = (void*)mp_txn;
            cmd_mp.is_cow_begin = false;
            cmd_mp.is_cow_end = false;
            DCHECK(mp_txn->lock_status.num_locks() > 0);
            auto lock_index = mp_txn->lock_status.get_lock_index_no_write(lock_id);
            DCHECK(lock_index != -1);
            cmd_mp.last_writer = mp_txn->lock_status.get_lock(lock_index).get_last_writer();
            DCHECK(cmd_mp.last_writer != std::numeric_limits<uint64_t>::max());
            cmd_mp.write_lock = mp_txn->lock_status.get_lock(lock_index).get_mode() == TransactionType::LockStatus::LockMode::WRITE;

            q.emplace_back(std::move(cmd_mp));
            auto & lock_bucket = get_lock_bucket(lock_id);
            if (lock_bucket.write_locked() == false) {
              DCHECK(granule_command_queue_processing[granule_to_cmd_queue_index[lock_id]] == false);
              replay_commands_in_granule(lock_id);
            }
            auto & q2 = get_granule_lock_request_queue(lock_id);
            if (q2.empty() == false) {
              granule_lock_reqeust_candidates.push_back(lock_id);
            }
          }
        }
      }
      DCHECK(first_lock_id_by_this_worker != -1);
      mp_txn->replay_queue_lock_id = first_lock_id_by_this_worker;
    };
    
    while(dec.size() > 0) {
      ++cmd_cnt;
      TxnCommandBase cmd;
      dec >> cmd.tid;
      dec >> cmd.is_coordinator;
      dec >> cmd.is_mp;
      dec >> cmd.position_in_log;
      dec >> cmd.partition_id;
      dec >> cmd.granule_id;
      dec >> cmd.last_writer;
      dec >> cmd.write_lock;
      dec >> cmd.is_cow_begin;
      dec >> cmd.is_cow_end;
      max_posisiton_in_log_this_batch = std::max(max_posisiton_in_log_this_batch, cmd.position_in_log);
      std::size_t command_data_size;
      dec >> command_data_size;
      std::string command_data = std::string(dec.get_raw_ptr(), command_data_size);
      dec.remove_prefix(command_data_size);
      if (cmd.is_cow_begin || cmd.is_cow_end) {
        std::string tag = cmd.is_cow_begin ? "COW_BEGIN" : "COW_END";
        
        ReplicaCheckpointMeta *ckpt_meta = new ReplicaCheckpointMeta{cmd.is_cow_begin, cmd.is_cow_end, 0};
        ckpt_meta->checkpoint_step = ++checkpoint_step;
        LOG(INFO) << "Replica worker " << this_cluster_worker_id << " found a " << tag << " at step " << ckpt_meta->checkpoint_step;
        for (auto lock_id : lock_ids_by_this_replica_executor) {
          auto lock_index = granule_to_cmd_queue_index[lock_id];
          auto & q = granule_command_queues[lock_index];
          auto cmd_copy = cmd;
          cmd.partition_id = lock_id_to_partition(lock_id);
          cmd.granule_id = lock_id_to_granule(lock_id);
          cmd_copy.txn = (void*)ckpt_meta;
          ckpt_meta->ref_cnt++;
          q.push_back(cmd_copy);
        }

        for (auto lock_id : lock_ids_by_this_replica_executor) {
          DCHECK(granule_command_queue_processing[granule_to_cmd_queue_index[lock_id]] == false);
          replay_commands_in_granule(lock_id);
          auto & q2 = get_granule_lock_request_queue(lock_id);
          if (q2.empty() == false) {
            granule_lock_reqeust_candidates.push_back(lock_id);
          }
        }
      } else if (!cmd.is_coordinator) { // place into one partition command queue for replay
        auto partition = cmd.partition_id;
        auto granule_id = cmd.granule_id;
        DCHECK(granule_id >= 0 && granule_id < (int)this->context.granules_per_partition);
        auto lock_id = to_lock_id(partition, granule_id);
        DCHECK(partition != -1);
        auto lock_index = granule_to_cmd_queue_index[lock_id];
        auto & q = granule_command_queues[lock_index];
        cmd.txn = nullptr;
        q.emplace_back(std::move(cmd));
        auto & lock_bucket = get_lock_bucket(lock_id);
        if (lock_bucket.write_locked() == false) {
          DCHECK(granule_command_queue_processing[granule_to_cmd_queue_index[lock_id]] == false);
          replay_commands_in_granule(lock_id);
        }
          
        auto & q2 = get_granule_lock_request_queue(lock_id);
        if (q2.empty() == false) {
          granule_lock_reqeust_candidates.push_back(lock_id);
        }
      } else if (cmd.is_mp == false) { // place into one partition command queue for replay
        CHECK(cmd.granule_id == -1);
        CHECK(cmd.partition_id == -1);
        CHECK(false);
        Decoder dec_sp_data(command_data);
        uint64_t bitmap_size;
        dec_sp_data >> bitmap_size;
        std::string bitmap_raw(dec_sp_data.get_raw_ptr(), bitmap_size);
        dec_sp_data.remove_prefix(bitmap_size);
        lock_bitmap * bm = lock_bitmap::deserialize_from_raw(bitmap_raw);
        while (dec_sp_data.size()) {
          uint64_t command_data_size;
          dec_sp_data >> command_data_size;
          std::string command_data(dec_sp_data.get_raw_ptr(), command_data_size);
          dec_sp_data.remove_prefix(command_data_size);
          auto mp_txn = this->workload.deserialize_from_raw(this->context, command_data).release();
          mp_txn->context = &this->context;
          CHECK(mp_txn->is_single_partition());
          bm->inc_ref();
          mp_txn->replay_bm_for_sp = bm;
          TxnCommandBase cmd_mp_txn = cmd;
          cmd_mp_txn.tid = mp_txn->transaction_id;
          cmd_mp_txn.is_coordinator = true;
          cmd_mp_txn.txn = (void*)mp_txn;
          cmd_mp_txn.is_mp = true;
          enqueue_mp_transaction(cmd_mp_txn, mp_txn);
        }
      } else { // place into multiple partition command queues for replay
        DCHECK(cmd.partition_id == -1);
        auto mp_txn = this->workload.deserialize_from_raw(this->context, command_data).release();
        mp_txn->startTime = std::chrono::steady_clock::now();
        mp_txn->context = &this->context;
        enqueue_mp_transaction(cmd, mp_txn);
        mp_count_this_batch++;
      }
      // if (mp_count_this_batch >= 15) {
      //   replay_points.push_back(std::make_pair(max_posisiton_in_log_this_batch, mp_count_this_batch));
      //   mp_count_this_batch = 0;
      //   max_posisiton_in_log_this_batch = 0;
      // }
    }
    DCHECK(dec.size() == 0);
    spread_cnt.add(cmd_cnt);
    if (mp_count_this_batch > 0) {
      replay_points.push_back(std::make_pair(max_posisiton_in_log_this_batch, mp_count_this_batch));
    }
  }

  void command_replication_request_handler(const Message & inputMessage, MessagePiece inputPiece,
                                   Message &responseMessage,
                                   ITable &table, Transaction *txn) {
    DCHECK(is_replica_worker);
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::COMMAND_REPLICATION_REQUEST));

    auto key_size = table.key_size();
    auto value_size = table.value_size();
    /*
     * The structure of a write command replication request: (ith_replica, txn data)
     * The structure of a write lock response: (success?, key offset, value?)
     */
    std::size_t ith_replica;
    int initiating_cluster_worker_id;
    bool persist_cmd_buffer = false;
    auto stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> ith_replica >> initiating_cluster_worker_id >> persist_cmd_buffer;
    DCHECK(initiating_cluster_worker_id == replica_cluster_worker_id);
    std::string data = dec.bytes.toString();
    DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(std::size_t) + data.size() + sizeof(initiating_cluster_worker_id) + sizeof(persist_cmd_buffer));
    std::size_t data_sz = data.size();

    ScopedTimer t([&, this](uint64_t us) {
      spread_time.add(us);
    });
    ReplayBuffer * replay_buf = new ReplayBuffer(std::move(data));
    replay_buffers.push_back(replay_buf);

    //spread_replicated_commands(data);
  }

  void command_replication_response_handler(const Message & inputMessage, MessagePiece inputPiece,
                                            Message &responseMessage,
                                            ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::COMMAND_REPLICATION_RESPONSE));
    DCHECK(is_replica_worker == false);
    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
  }

  void acquire_lock_and_read_response_handler(const Message & inputMessage, MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::ACQUIRE_LOCK_AND_READ_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a read lock response: (success?, key offset, value?)
     */

    char success;
    uint32_t key_offset;
    uint64_t last_writer;
    bool write_lock, requested_lock;
    uint64_t ts1, ts2, ts3;
    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> success >> key_offset >> last_writer >> write_lock >> requested_lock >> ts1 >> ts2 >> ts3;
    //rtt_stat.add(std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()).time_since_epoch().count() - ts1);
    auto lock_index = txn->readSet[key_offset].get_lock_index();
    DCHECK(lock_index != -1);
    auto & lstatus = txn->lock_status.get_lock(lock_index);
    if (success == 1 && (lstatus.get_success() == TransactionType::LockStatus::SuccessState::REQUESTED || lstatus.get_success() == TransactionType::LockStatus::SuccessState::SUCCEED)) {
      uint32_t msg_length = inputPiece.get_message_length();
      auto header_size = MessagePiece::get_header_size() ;
      uint32_t exp_length = header_size + sizeof(success) +
                 sizeof(key_offset) + sizeof(last_writer) + sizeof(write_lock) + sizeof(requested_lock) + sizeof(uint64_t) * 3 + value_size;
      DCHECK(msg_length == exp_length);

      TwoPLRWKey &readKey = txn->readSet[key_offset];
      dec.read_n_bytes(readKey.get_value(), value_size);
      if (write_lock) {
        readKey.set_write_lock_bit();
      } else {
        readKey.set_read_lock_bit();
      }
      if (requested_lock) {
        DCHECK(lstatus.get_success() == TransactionType::LockStatus::SuccessState::REQUESTED);
        if (is_replica_worker == false) {
          DCHECK(last_writer != txn->transaction_id);
          lstatus.set_last_writer(last_writer);
        } else {
          if (write_lock) {
            DCHECK(last_writer == txn->transaction_id);
          } else {
            DCHECK(last_writer != txn->transaction_id);
          }
          DCHECK(lstatus.get_last_writer() != txn->transaction_id);
        }
      }
      lstatus.set_success(TransactionType::LockStatus::SuccessState::SUCCEED);
    } else {
      // DCHECK(inputPiece.get_message_length() ==
      //        MessagePiece::get_header_size() + sizeof(last_writer) + sizeof(write_lock) + sizeof(requested_lock) + sizeof(uint64_t) * 3 + sizeof(success) +
      //            sizeof(key_offset));
      if (success == 2) {
        txn->abort_lock_owned_by_others++;
        txn->abort_lock_queue_len_sum += ts3;
      } else if (success == 0) {
        //DCHECK(success == 0);
        txn->abort_lock_owned_by_no_one++;
      }
      txn->abort_lock = true;
      DCHECK(lstatus.get_success() == TransactionType::LockStatus::SuccessState::REQUESTED || lstatus.get_success() == TransactionType::LockStatus::SuccessState::FAILED);
      lstatus.set_success(TransactionType::LockStatus::SuccessState::FAILED);
    }
    txn->pendingResponses--;
    DCHECK(txn->pendingResponses >= 0);
    txn->network_size += inputPiece.get_message_length();
    if (txn->lock_request_responded == false) {
      txn->lock_request_responded = true;
      // txn->first_lock_request_arrive_latency = ts2 - std::chrono::time_point_cast<std::chrono::microseconds>(txn->lock_issue_time).time_since_epoch().count();
      // txn->first_lock_request_processed_latency = ts3 - std::chrono::time_point_cast<std::chrono::microseconds>(txn->lock_issue_time).time_since_epoch().count();
      
      // txn->first_lock_response_latency = std::chrono::duration_cast<std::chrono::microseconds>(
      //         std::chrono::steady_clock::now() - txn->lock_issue_time)
      //         .count();
    }
    // LOG(INFO) << "acquire_partition_lock_response for worker " << this_cluster_worker_id
    //           << " on partition " << partition_id
    //           << " partition locked acquired " << success 
    //           << " pending responses " << txn->pendingResponses;
  }

  void write_back_request_handler(const Message & inputMessage, MessagePiece inputPiece,
                                  Message &responseMessage,
                                  ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::WRITE_BACK_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();
    uint64_t tid = inputMessage.get_transaction_id();
    /*
     * The structure of a write request: (request_remote_worker, primary key, field value)
     * The structure of a write response: (success?)
     */
    uint32_t request_remote_worker, granule_id;

    auto stringPiece = inputPiece.toStringPiece();
    uint64_t commit_tid;
    bool persist_commit_record;
    std::size_t ith_replica;

    Decoder dec(stringPiece);
    dec >> commit_tid >> persist_commit_record >> request_remote_worker >> granule_id >> ith_replica;

    DCHECK(granule_id == inputPiece.get_granule_id());
    if (ith_replica)
      DCHECK(is_replica_worker);
    bool success = false;
    DCHECK(this_cluster_worker_id == (int)partition_owner_cluster_worker(partition_id, ith_replica));

    auto lock_id = to_lock_id(partition_id, granule_id);
    auto & lock_bucket = get_lock_bucket(lock_id);
    // Make sure the partition is currently owned by request_remote_worker
    DCHECK(lock_bucket.write_locked());
    DCHECK(lock_bucket.get_last_writer() == tid);
    success = true;
    // if (lock_bucket == tid) {
    //   success = true;
    // }

    // LOG(INFO) << "write_back_request_handler for worker " << request_remote_worker
    //   << " on partition " << lock_id
    //   << " partition locked acquired " << success
    //   << " current partition owner " << tid_to_string(lock_bucket)
    //   << " ith_replica " << ith_replica << " txn " << tid_to_string(inputMessage.get_transaction_id());

    //DCHECK(lock_bucket == tid);

    if (success) {
      stringPiece = dec.bytes;
      DCHECK(inputPiece.get_message_length() ==
      MessagePiece::get_header_size() + sizeof(ith_replica) + sizeof(granule_id) + sizeof(commit_tid) + sizeof(persist_commit_record) + key_size + field_size + sizeof(uint32_t));
      const void *key = stringPiece.data();
      stringPiece.remove_prefix(key_size);
      table.deserialize_value(key, stringPiece);
    }

    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + sizeof(bool);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::WRITE_BACK_RESPONSE), message_size,
        table_id, partition_id, granule_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header << success;
    if (ith_replica > 0)
      responseMessage.set_is_replica(true);
    responseMessage.set_transaction_id(inputMessage.get_transaction_id());
    responseMessage.flush();
    responseMessage.set_gen_time(Time::now());

    if (persist_commit_record) {
      DCHECK(this->logger);
      std::ostringstream ss;
      ss << commit_tid << true;
      auto output = ss.str();
      auto lsn = this->logger->write(output.c_str(), output.size(), false, [&, this](){ handle_requests(); });
      //txn->get_logger()->sync(lsn, );
    }
  }


  void write_back_response_handler(const Message & inputMessage, MessagePiece inputPiece,
                                    Message &responseMessage,
                                    ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::WRITE_BACK_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a partition write and release response: (success?)
     */

    bool success;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> success;
    
    DCHECK(success);
    if (success) {
      DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(success));
    } else {
      DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(success));
      txn->abort_lock = true;
    }

    //txn->pendingResponses--;
    //txn->network_size += inputPiece.get_message_length();
    // LOG(INFO) << "write_back_response_handler for worker " << this_cluster_worker_id
    //   << " on partition " << partition_id
    //   << " remote partition locked released " << success 
    //   << " pending responses " << txn->pendingResponses;
  }

  static std::size_t new_get_replayed_log_posistion_response_message(Message &responseMessage, int64_t replayed_posisiton) {
    auto message_size = MessagePiece::get_header_size() + sizeof(replayed_posisiton);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::GET_REPLAYED_LOG_POSITION_RESPONSE), message_size,
        0, 0);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header << replayed_posisiton;
    responseMessage.set_transaction_id(0);
    responseMessage.set_is_replica(false);
    responseMessage.flush();
    return message_size;
  }

  int64_t active_replica_waiting_position = -1;

  int replcia_txn_group_start_no = 0;
  void respond_to_active_replica_with_replay_position(int64_t replayed_position) {
    if (active_replica_waiting_position == -1)
      return;
    if (replayed_position < active_replica_waiting_position)
      return;
    DCHECK(0 <= replica_cluster_worker_id && replica_cluster_worker_id < cluster_worker_num);
    new_get_replayed_log_posistion_response_message(*cluster_worker_messages[replica_cluster_worker_id], replayed_position);
    add_outgoing_message(replica_cluster_worker_id);
    flush_messages();
    last_mp_arrival = std::chrono::steady_clock::now();
    last_commit = std::chrono::steady_clock::now();
    active_replica_waiting_position = -1;
    replcia_txn_group_start_no = 0;
  }

  void rtt_test_request_handler(const Message & inputMessage, MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::RTT_REQUEST));
    DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(int) + sizeof(int));
    int cluster_worker_id, ith_replica;
    auto stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> cluster_worker_id >> ith_replica;

    auto message_size = MessagePiece::get_header_size();
    auto message_piece_header = MessagePiece::construct_message_piece_header(
      static_cast<uint32_t>(HStoreMessage::RTT_RESPONSE), message_size,
      0, 0);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header;
    responseMessage.set_transaction_id(inputMessage.get_transaction_id());
    responseMessage.set_is_replica(ith_replica > 0);
    responseMessage.flush();
    responseMessage.set_gen_time(Time::now());
  }

  void get_replayed_log_position_request_handler(const Message & inputMessage, MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    ScopedTimer t([&, this](uint64_t us) {
      replay_query_time.add(us);
    });
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::GET_REPLAYED_LOG_POSITION_REQUEST));
    DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(int) + sizeof(int) + sizeof(int64_t));
    DCHECK(is_replica_worker);
    int cluster_worker_id, ith_replica;
    auto stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    int64_t active_replica_waiting_position_tmp;
    dec >> active_replica_waiting_position_tmp >> cluster_worker_id >> ith_replica;
    int64_t minimum_replayed_log_positition = get_minimum_replayed_log_position();
    //DCHECK(active_replica_waiting_position == -1);
    //LOG(INFO) << "cluster_worker " << cluster_worker_id << " called persist_cmd_buffer_request_handler on cluster worker " << this_cluster_worker_id;
    if (minimum_replayed_log_positition >= active_replica_waiting_position_tmp) {
      auto message_size = MessagePiece::get_header_size() + sizeof(minimum_replayed_log_positition);
      auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::GET_REPLAYED_LOG_POSITION_RESPONSE), message_size,
        0, 0);

      star::Encoder encoder(responseMessage.data);
      encoder << message_piece_header << minimum_replayed_log_positition;
      responseMessage.set_transaction_id(inputMessage.get_transaction_id());
      responseMessage.set_is_replica(false);
      responseMessage.flush();
      responseMessage.set_gen_time(Time::now());
    } else {
      //DCHECK(active_replica_waiting_position == -1);
      active_replica_waiting_position = active_replica_waiting_position_tmp;
      last_mp_arrival = std::chrono::steady_clock::now();
      last_commit = std::chrono::steady_clock::now();
    }
  }
  
  void get_replayed_log_position_response_handler(const Message & inputMessage, MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::GET_REPLAYED_LOG_POSITION_RESPONSE));
    DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(int64_t));
    DCHECK(is_replica_worker == false);
    auto stringPiece = inputPiece.toStringPiece();

    Decoder dec(stringPiece);
    int64_t min_replayed_log_position_in_replica;
    dec >> min_replayed_log_position_in_replica;
    DCHECK(minimum_replayed_log_position <= min_replayed_log_position_in_replica);
    minimum_replayed_log_position = min_replayed_log_position_in_replica;
    get_replica_replay_log_position_responses++;
  }

  void persist_cmd_buffer_request_handler(const Message & inputMessage, MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::PERSIST_CMD_BUFFER_REQUEST));
    DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(int) + sizeof(int));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    persist_and_clear_command_buffer();
    int cluster_worker_id, ith_replica;
    auto stringPiece = inputPiece.toStringPiece();

    Decoder dec(stringPiece);
    dec >> cluster_worker_id >> ith_replica;
    //LOG(INFO) << "cluster_worker " << cluster_worker_id << " called persist_cmd_buffer_request_handler on cluster worker " << this_cluster_worker_id;
    auto message_size = MessagePiece::get_header_size() + sizeof(this_cluster_worker_id);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::PERSIST_CMD_BUFFER_RESPONSE), message_size,
        table_id, partition_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header << this_cluster_worker_id;
    responseMessage.set_transaction_id(inputMessage.get_transaction_id());
    responseMessage.set_is_replica(ith_replica > 0);
    responseMessage.flush();
    responseMessage.set_gen_time(Time::now());
  }

  void persist_cmd_buffer_response_handler(const Message & inputMessage, MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::PERSIST_CMD_BUFFER_RESPONSE));
    DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(int));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    auto stringPiece = inputPiece.toStringPiece();
    int cluster_worker_id;
    Decoder dec(stringPiece);
    dec >> cluster_worker_id;
    this->received_persist_cmd_buffer_responses++;
    //LOG(INFO) << "cluster_worker " << this_cluster_worker_id << " received response from persist_cmd_buffer_request_handler on cluster worker " << cluster_worker_id << ", received_persist_cmd_buffer_responses " << received_persist_cmd_buffer_responses;
  }

  void write_participant_command_request_handler(const Message & inputMessage, MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::WRITE_PARTICIPANT_COMMAND_REQUEST)); 
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    uint64_t tid = inputMessage.get_transaction_id();
    /*
     * The structure of a release partition lock request: (request_remote_worker, sync, ith_replica, write_cmd_buffer)
     * No response.
     */
    uint32_t request_remote_worker;
    uint32_t granule_id;
    uint64_t requested_last_writer;
    bool write_lock;
    bool write_cmd_buffer;
    std::size_t ith_replica;

    auto stringPiece = inputPiece.toStringPiece();

    Decoder dec(stringPiece);
    dec >> request_remote_worker >> granule_id >> requested_last_writer >> write_lock  >> ith_replica;
    DCHECK(ith_replica == 0); // Only for primary replica 
    auto lock_id = to_lock_id(partition_id, granule_id);
    auto & lock_bucket = get_lock_bucket(lock_id);
    //auto last_writer = lock_bucket.get_last_writer();
    DCHECK(granule_id == inputPiece.get_granule_id());
    std::string txn_command_data;
    std::size_t txn_command_data_size;
    int partitionId;
    bool is_mp;
    uint64_t transactionId;
    dec >> partitionId;
    dec >> transactionId;
    dec >> is_mp;
    dec >> txn_command_data_size;
    if (txn_command_data_size) {
      txn_command_data = std::string(dec.bytes.data(), txn_command_data_size);
    }
    DCHECK(transactionId == tid);
    bool is_coordinator = false;
    DCHECK(is_mp);
    DCHECK(partitionId != -1);
    int64_t position_in_log = next_position_in_command_log++;
    size_t command_buffer_data_size = command_buffer_data.size();
    Encoder enc(command_buffer_data);
    bool is_cow_begin = false;
    bool is_cow_end = false;
    enc << tid;
    enc << is_coordinator;
    enc << is_mp;
    enc << position_in_log;
    enc << partition_id;
    enc << granule_id;
    enc << requested_last_writer;
    enc << write_lock;
    enc << is_cow_begin;
    enc << is_cow_end;
    if (write_lock) {
      DCHECK(lock_bucket.get_last_writer() == requested_last_writer);
      DCHECK(requested_last_writer == tid);
    } else {
      DCHECK(lock_bucket.get_last_writer() != tid);
    }
    enc << txn_command_data_size;
    enc.write_n_bytes(txn_command_data.data(), txn_command_data_size);
    if (this->replica_num > 1) {
      command_buffer_outgoing_data.insert(command_buffer_outgoing_data.end(), command_buffer_data.begin() + command_buffer_data_size, command_buffer_data.end());
    }
  }

  void release_lock_request_handler(const Message & inputMessage, MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::RELEASE_LOCK_REQUEST));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto field_size = table.field_size();

    uint64_t tid = inputMessage.get_transaction_id();
    /*
     * The structure of a release partition lock request: (request_remote_worker, sync, ith_replica, write_cmd_buffer)
     * No response.
     */
    uint32_t request_remote_worker;
    uint32_t granule_id;
    uint64_t requested_last_writer;
    bool write_lock;
    bool sync, write_cmd_buffer;
    std::size_t ith_replica;

    auto stringPiece = inputPiece.toStringPiece();

    Decoder dec(stringPiece);
    dec >> request_remote_worker >> granule_id >> requested_last_writer >> write_lock >> sync >> ith_replica >> write_cmd_buffer;
    auto lock_id = to_lock_id(partition_id, granule_id);
    auto & lock_bucket = get_lock_bucket(lock_id);
    //auto last_writer = lock_bucket.get_last_writer();
    DCHECK(granule_id == inputPiece.get_granule_id());
    if (write_cmd_buffer) {
      std::string txn_command_data;
      std::size_t txn_command_data_size;
      int partition_id;
      bool is_mp;
      uint64_t tid;
      dec >> partition_id;
      dec >> tid;
      dec >> is_mp;
      dec >> txn_command_data_size;
      if (txn_command_data_size) {
        txn_command_data = std::string(dec.bytes.data(), txn_command_data_size);
      }
      bool is_coordinator = false;
      DCHECK(is_mp);
      DCHECK(partition_id != -1);
      int64_t position_in_log = next_position_in_command_log++;
      size_t command_buffer_data_size = command_buffer_data.size();
      Encoder enc(command_buffer_data);
      bool is_cow_begin = false;
      bool is_cow_end = false;
      enc << tid;
      enc << is_coordinator;
      enc << is_mp;
      enc << position_in_log;
      enc << partition_id;
      enc << granule_id;
      enc << requested_last_writer;
      enc << write_lock;
      enc << is_cow_begin;
      enc << is_cow_end;
      if (write_lock) {
        DCHECK(lock_bucket.get_last_writer() == requested_last_writer);
        DCHECK(requested_last_writer == tid);
      } else {
        DCHECK(lock_bucket.get_last_writer() != tid);
      }
      enc << txn_command_data_size;
      enc.write_n_bytes(txn_command_data.data(), txn_command_data_size);
      if (this->replica_num > 1) {
        command_buffer_outgoing_data.insert(command_buffer_outgoing_data.end(), command_buffer_data.begin() + command_buffer_data_size, command_buffer_data.end());
      }
    }

    DCHECK(this_cluster_worker_id == (int)partition_owner_cluster_worker(partition_id, ith_replica));
    if (ith_replica > 0)
      DCHECK(is_replica_worker);
    bool success;
    
    // if (lock_bucket != tid) {
    //   success = false;
    // } else {
    //   // if (lock_bucket != -1)
    //   //   LOG(INFO) << "Partition " << lock_id_to_string(lock_id) << " unlocked by cluster worker" << request_remote_worker << " by this_cluster_worker_id " << this_cluster_worker_id << " ith_replica " << ith_replica << " txn " << tid_to_string(tid);
    //   DCHECK(lock_bm.get_bit(lock_id) == true);
    //   lock_bucket = -1;
    //   lock_bm.clear_bit(lock_id);
    //   DCHECK(lock_bm.get_bit(lock_id) == false);
    //   success = true;
    // }
    if (write_lock == false) {
      if (is_replica_worker == false) {
        DCHECK(lock_bucket.reader_cnt() >= 1);
      }
      lock_bucket.dec_reader_cnt();
      success = true;
    } else {
      DCHECK(lock_bucket.write_locked());
      DCHECK(lock_bucket.get_last_writer() == tid);
      DCHECK(lock_bucket.has_reader() == false);
      if (is_replica_worker == false) {
        lock_bucket.set_last_writer(requested_last_writer);
      }
      lock_bucket.clear_write_lock();
      success = true;
    }

    if (is_replica_worker) {
      auto & q = get_granule_lock_cmd_queue(lock_id);
      if (q.empty() == false) {
        DCHECK(granule_command_queue_processing[granule_to_cmd_queue_index[lock_id]] == false);
        replay_commands_in_granule(lock_id);
      }
      auto & q2 = get_granule_lock_request_queue(lock_id);
      if (q2.empty() == false) {
        granule_lock_reqeust_candidates.push_back(lock_id);
      }
    }
    // if (ith_replica > 0);
    //   DCHECK(success)
    // LOG(INFO) << "release_lock_request_handler from worker " << this_cluster_worker_id
    //   << " on partition " << lock_id << " " << lock_id_to_string(lock_id) << " by " << tid_to_string(tid)
    //   << ", lock released " << success;
    if (!sync)
      return;
    // prepare response message header
    auto message_size = MessagePiece::get_header_size() + sizeof(bool);
    auto message_piece_header = MessagePiece::construct_message_piece_header(
        static_cast<uint32_t>(HStoreMessage::RELEASE_LOCK_RESPONSE), message_size,
        table_id, partition_id, granule_id);

    star::Encoder encoder(responseMessage.data);
    encoder << message_piece_header << success;
    if (ith_replica > 0)
      responseMessage.set_is_replica(true);
    responseMessage.set_transaction_id(inputMessage.get_transaction_id());
    responseMessage.flush();
    responseMessage.set_gen_time(Time::now());
  }

  void release_lock_response_handler(const Message & inputMessage, MessagePiece inputPiece,
                                          Message &responseMessage,
                                          ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::RELEASE_LOCK_RESPONSE));
    auto table_id = inputPiece.get_table_id();
    auto partition_id = inputPiece.get_partition_id();
    DCHECK(table_id == table.tableID());
    DCHECK(partition_id == table.partitionID());
    auto key_size = table.key_size();
    auto value_size = table.value_size();

    /*
     * The structure of a partition write and release response: (success?)
     */

    bool success;

    StringPiece stringPiece = inputPiece.toStringPiece();
    Decoder dec(stringPiece);
    dec >> success;
    
    DCHECK(success);
    if (success) {
      DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(success));
    } else {
      DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(success));
      txn->abort_lock = true;
    }

    txn->pendingResponses--;
    txn->network_size += inputPiece.get_message_length();
    // LOG(INFO) << "release_lock_response_handler for worker " << this_cluster_worker_id
    //   << " on partition " << partition_id
    //   << " remote partition locked released " << success 
    //   << " pending responses " << txn->pendingResponses;
  }
  void fill_pending_txns(std::size_t limit) {
    while (pending_txns.size() < limit) {
      auto t = get_next_transaction();
      if (t == nullptr)
        break;
      pending_txns.push_back(t);
    }
  }

  void persist_and_clear_command_buffer(std::function<void()> work_while_io) {
    if (command_buffer_data.empty())
      return;
    std::string data;
    std::swap(command_buffer_data, data);
    command_buffer_data.clear();
    this->logger->write(data.data(), data.size(), true, work_while_io);
  }

  void persist_and_clear_command_buffer(bool continue_work = false) {
    if (command_buffer_data.empty())
      return;
    std::string data;
    std::swap(command_buffer_data, data);
    // Encoder encoder(data);
    // for (size_t i = 0; i < command_buffer.size(); ++i) {
    //   encoder << command_buffer[i].tid;
    //   encoder << command_buffer[i].partition_id;
    //   encoder << command_buffer[i].is_mp;
    //   encoder << command_buffer[i].position_in_log;
    //   encoder << command_buffer[i].command_data.size();
    //   encoder.write_n_bytes(command_buffer[i].command_data.data(), command_buffer[i].command_data.size());
    // }
    command_buffer_data.clear();
    // if (replica_num > 1 && is_replica_worker == false && command_buffer_outgoing_data.empty() == false) {
    //   if (this->context.lotus_async_repl == false) {
    //     send_commands_to_replica();
    //   }
    // }
    if (continue_work) {
      this->logger->write(data.data(), data.size(), true, [&, this](){handle_requests(false);});
    } else {
      this->logger->write(data.data(), data.size(), true, [&, this](){});
    }
  }

  bool process_single_transaction(TransactionType * txn, bool lock_acquired = false) {
    auto txn_id = txn->transaction_id;
    // if (txn->is_single_partition() && lock_acquired == false && this->context.granules_per_partition == 1) {
    //   DCHECK(false);
    //   // auto partition_count = txn->get_partition_count();
    //   // auto partition_id = txn->get_partition(0);
    //   // DCHECK(partition_count == 1);
    //   // auto granule_count = txn->get_partition_granule_count(0);
    //   // if (granule_count == 1) {
    //   //   auto granule_id = txn->get_granule(0, 0);
    //   //   auto lock_id = to_lock_id(partition_id, granule_id);
    //   //   if (lock_buckets[lock_id] != -1) {
    //   //     return false;
    //   //   } else {
    //   //     DCHECK(lock_bm.get_bit(lock_id) == false);
    //   //     lock_buckets[lock_id] = txn_id;
    //   //     lock_bm.set_bit(lock_id);
    //   //     DCHECK(lock_bm.get_bit(lock_id) == true);
    //   //   }
    //   // } else {
    //   //   for (int j = 0; j < granule_count; ++j) {
    //   //     auto granule_id = txn->get_granule(0, j);
    //   //     auto lock_id = to_lock_id(partition_id, granule_id);
    //   //     if (lock_buckets[lock_id] != -1) {
    //   //       return false;
    //   //     }
    //   //   }
    //   //   for (int j = 0; j < granule_count; ++j) {
    //   //     auto granule_id = txn->get_granule(0, j);
    //   //     auto lock_id = to_lock_id(partition_id, granule_id);
    //   //     DCHECK(lock_buckets[lock_id] == -1);
    //   //     lock_buckets[lock_id] = txn_id;
    //   //     lock_bm.set_bit(lock_id);
    //   //     DCHECK(lock_bm.get_bit(lock_id) == true);
    //   //   }
    //   // }
    // }
    setupHandlers(*txn);
    DCHECK(txn->execution_phase == false);
    
    active_txns[txn->transaction_id] = txn;
    auto ltc =
    std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - txn->startTime)
        .count();
    txn->set_stall_time(ltc);

    auto result = txn->execute(this->id);
    if (result == TransactionResult::READY_TO_COMMIT) {
      DCHECK(check_granule_set(txn));
      bool commit;
      {
        ScopedTimer t([&, this](uint64_t us) {
          if (commit) {
            txn->record_commit_work_time(us);
          } else {
            auto ltc =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - txn->startTime)
                .count();
            txn->set_stall_time(ltc);
          }
        });
        commit = this->commit_transaction(*txn, cluster_worker_messages);
      }
      ////LOG(INFO) << "Txn Execution result " << (int)result << " commit " << commit;
      this->n_network_size.fetch_add(txn->network_size);
      if (commit) {
        ++worker_commit;
        this->n_commit.fetch_add(1);
        if (txn->si_in_serializable) {
          this->n_si_in_serializable.fetch_add(1);
        }
        active_txns.erase(txn->transaction_id);
        commit_interval.add(std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - last_commit)
              .count());
        last_commit = std::chrono::steady_clock::now();
        return true;
      } else {
//        DCHECK(txn->is_single_partition() == false);
        // if (is_replica_worker)
        //   LOG(INFO) << "Txn " << txn_id << " Execution result " << (int)result << " abort by lock conflict on cluster worker " << this_cluster_worker_id;
        // Txns on slave replicas won't abort due to locking failure.
        if (txn->abort_lock) {
          this->n_abort_lock.fetch_add(1);
        } else {
          DCHECK(txn->abort_read_validation);
          this->n_abort_read_validation.fetch_add(1);
        }
        if (this->context.sleep_on_retry) {
          // std::this_thread::sleep_for(std::chrono::milliseconds(
          //     this->random.uniform_dist(100, 1000)));
        }
        //retry_transaction = true;
        active_txns.erase(txn->transaction_id);
        txn->reset();
        return false;
      }
    } else {
      if (is_replica_worker == false && txn->abort_lock_lock_released == false) {
        // We only release locks when executing on active replica
        abort(*txn, cluster_worker_messages);
      }
      DCHECK(result == TransactionResult::ABORT_NORETRY);
      txn->abort_no_retry = true;
      txn->finished_commit_phase = true;
      active_txns.erase(txn->transaction_id);
      return false;
    }
    return true;
  }

  std::size_t process_to_commit(std::deque<TransactionType*> & to_commit, std::function<void(TransactionType*)> post_commit = [](TransactionType*){}) {
    std::size_t cnt = 0;
    while (!to_commit.empty()) {
      auto txn = to_commit.front();
      to_commit.pop_front();
      txns_replayed++;
      DCHECK(txn->pendingResponses == 0);
      if (txn->finished_commit_phase) {
        DCHECK(txn->pendingResponses == 0);
        post_commit(txn);
        continue;
      }
      process_single_txn_commit(txn);
      post_commit(txn);
      cnt ++;
      handle_requests(false);
    }
    return cnt;
  }

  void process_execution_async_single_mp_txn(TransactionType* txn) {
    setupHandlers(*txn);
    //txn->reset();
    DCHECK(txn->lock_status.num_locks() > 0);
    txn->execution_phase = false;
    txn->synchronous = false;
    active_txns[txn->transaction_id] = txn;
    auto res = txn->execute(this->id);
    txn->lock_issue_time = std::chrono::steady_clock::now();
    auto ltc =
    std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - txn->startTime)
        .count();
    txn->execution_done_latency = ltc;
    DCHECK(res != TransactionResult::ABORT_NORETRY);
    if (txn->pendingResponses == 0) {
      txn->abort_lock_local_read = true;
      async_txns_to_commit.push_back(txn);
      if (txn->abort_lock == false) {
        // auto ltc = std::chrono::duration_cast<std::chrono::microseconds>(
        // std::chrono::steady_clock::now() - txn->startTime)
        // .count();
        // tries_lock_response_latency[std::min((int)txn->tries, (int)tries_lock_response_latency.size() - 1)].add(ltc);
      }
    }
  }

  void execute_transaction(TransactionType* txn) {
    setupHandlers(*txn);
    txn->reset();
    txn->execution_phase = false;
    txn->synchronous = false;
    DCHECK(txn->is_single_partition() == false);
    // initiate read requests (could be remote)
    if (txn->tries == 1)
      txn->lock_issue_time = std::chrono::steady_clock::now();
    active_txns[txn->transaction_id] = txn;
    auto ltc = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - txn->startTime)
        .count();
    txn->set_stall_time(ltc);
    auto res = txn->execute(this->id);
    if (res == TransactionResult::ABORT_NORETRY) {
      txn->abort_no_retry = true;
    } else {
      DCHECK(check_granule_set(txn));
    }
    if (is_replica_worker == false && (txn->abort_lock || txn->abort_no_retry)) {
      // Early lock release
      abort(*txn, cluster_worker_messages);
    }
    if (txn->pendingResponses == 0) {
      async_txns_to_commit.push_back(txn);
    }
    if (async_txns_to_commit.empty() == false) {
      process_to_commit(async_txns_to_commit);
    }
  }

  int mp_type(TransactionType * txn) {
    int cnt_partition_local = 0;
    int cnt_partition_process = 0;
    int cnt_partition_node = 0;
    for (int32_t i = 0; i < txn->get_partition_count(); ++i) {
      int partition = txn->get_partition(i);
      if (partition_owner_cluster_worker(partition, txn->ith_replica) == this_cluster_worker_id) {
        cnt_partition_local++;
      } else if (partition_owner_cluster_coordinator(partition, txn->ith_replica) == this->context.coordinator_id) {
        cnt_partition_process++;
      } else {
        cnt_partition_node++;
      }
    }
    if (cnt_partition_node) {
      return 0;
    } else if (cnt_partition_process) {
      return 1;
    } else {
      return 2;
    }
  }

  int is_cross_node_mp(TransactionType * txn) {
    auto t = mp_type(txn); 
    return t == 0;
  }
  int is_cross_process_mp(TransactionType * txn) {
    auto t = mp_type(txn); 
    return t == 1;
  }

  void process_single_txn_commit(TransactionType * txn) {
    // auto ltc =
    // std::chrono::duration_cast<std::chrono::microseconds>(
    //     std::chrono::steady_clock::now() - txn->startTime)
    //     .count();
    // txn->commit_initiated_latency = ltc;
    DCHECK(txn->pendingResponses == 0);
    if (txn->abort_no_retry) {
      if (is_replica_worker == false && txn->abort_lock_lock_released == false) {
        // We only release locks when executing on active replica
        abort(*txn, cluster_worker_messages);
      }
      active_txns.erase(txn->transaction_id);
      txn->finished_commit_phase = true;
      return;
    }

    //DCHECK(check_granule_set(txn));
    if (txn->abort_lock) {
      if (is_replica_worker == false && txn->abort_lock_lock_released == false) {
        // We only release locks when executing on active replica
        abort(*txn, cluster_worker_messages);
      }
      
      if (txn->abort_lock_local_read) {
        this->n_abort_read_validation.fetch_add(1);
      } else {
        this->n_abort_lock.fetch_add(1);
      }
      active_txns.erase(txn->transaction_id);
      txn->finished_commit_phase = true;
      return;
    }

    txn->execution_phase = true;
    txn->synchronous = false;

    // fill in the writes
    if (txn->abort_lock == false) {
      write_command(*txn);
      if (this->context.lotus_async_repl == false) {
        send_commands_to_replica(true);
      }
    }
  
    auto result = txn->execute(this->id);
    DCHECK(txn->abort_lock == false);
    DCHECK(result == TransactionResult::READY_TO_COMMIT);
    bool commit;
    {
      ScopedTimer t([&, this](uint64_t us) {
        if (commit) {
          txn->record_commit_work_time(us);
        } else {
          auto ltc =
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - txn->startTime)
              .count();
          txn->set_stall_time(ltc);
        }
      });
      commit = this->commit_transaction(*txn, cluster_worker_messages);
    }
    ////LOG(INFO) << "Txn Execution result " << (int)result << " commit " << commit;
    this->n_network_size.fetch_add(txn->network_size);
    if (commit) {
      DCHECK(check_granule_set(txn));
      // if (is_cross_node_mp(txn))
      //   txn_retries.add(txn->tries);
      ++worker_commit;
      this->n_commit.fetch_add(1);
      if (txn->si_in_serializable) {
        this->n_si_in_serializable.fetch_add(1);
      }
      active_txns.erase(txn->transaction_id);
      commit_interval.add(std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - last_commit)
            .count());
      last_commit = std::chrono::steady_clock::now();
    } else {
      DCHECK(false);
      // if (is_replica_worker)
      //   LOG(INFO) << "Txn " << txn_id << " Execution result " << (int)result << " abort by lock conflict on cluster worker " << this_cluster_worker_id;
      // Txns on slave replicas won't abort due to locking failure.
      if (txn->abort_lock) {
        this->n_abort_lock.fetch_add(1);
      } else {
        DCHECK(txn->abort_read_validation);
        this->n_abort_read_validation.fetch_add(1);
      }
      if (this->context.sleep_on_retry) {
        // std::this_thread::sleep_for(std::chrono::milliseconds(
        //     this->random.uniform_dist(100, 1000)));
      }
      //retry_transaction = true;
      active_txns.erase(txn->transaction_id);
    }
    txn->finished_commit_phase = true;
  }

  void process_commit_phase() {
    int cnt = 0;
    while (active_txns.size()) {
      handle_requests(false);
      process_to_commit(async_txns_to_commit);
      if (replica_num > 1 && is_replica_worker == false && command_buffer_outgoing_data.empty() == false) {
        if (this->context.lotus_async_repl == false) {
          send_commands_to_replica();
        }
      }
    }
  }

  std::set<int> get_granule_set_from_query(TransactionType* txn) {
    std::set<int> lock_ids1;
    int partition_count = txn->get_partition_count();
    for (int i = 0; i < partition_count; ++i) {
      int partition_id = txn->get_partition(i);
      int granules_count = txn->get_partition_granule_count(i);
      for (int j = 0; j < granules_count; ++j) {
        int granule_id = txn->get_granule(i, j);
        int lock_id = to_lock_id(partition_id, granule_id);
        lock_ids1.insert(lock_id);
      }
    }
    return lock_ids1;
  }

  std::set<int> get_granule_set_from_rset(TransactionType* txn) {
    std::set<int> lock_ids2;
    for (auto & key : txn->readSet) {
      if (key.get_local_index_read_bit())
        continue;
      auto lock_id = to_lock_id(key.get_partition_id(), key.get_granule_id());
      lock_ids2.insert(lock_id);
    }

    // for (auto & key : txn->writeSet) {
    //   if (key.get_local_index_read_bit())
    //     continue;
    //   auto lock_id = to_lock_id(key.get_partition_id(), key.get_granule_id());
    //   lock_ids2.insert(lock_id);
    // }
    return lock_ids2;
  }

  bool check_granule_set(TransactionType* txn) {
    auto s1 = get_granule_set_from_query(txn);
    auto s2 = get_granule_set_from_rset(txn);
    bool res = s1 == s2;
    DCHECK(res);
    return res;
  }

  std::deque<double> mp_abort_rate;
  std::size_t abort_rate_window_size = 10;
  int control_step = 0;
  double abort_rate_threshold = 1;
  void process_mp_transactions(std::vector<TransactionType*> & mp_txns) {
    {
      for (size_t i = 0; i < mp_txns.size(); ++i) {
        execute_transaction(mp_txns[i]);
        //handle_requests(false);
      }
    }
    
    {
      process_commit_phase();
    }
  }

  std::vector<int> hstore_active_active_granules_locked;

  void find_partition_owners(std::vector<Transaction*> txns, std::vector<bool> & workers_need_persist_cmd_buffer) {
    for (auto txn : txns) {
      int partition_count = txn->get_partition_count();
      for (int i = 0; i < partition_count; ++i) {
        int partitionId = txn->get_partition(i);
        int granules_count = txn->get_partition_granule_count(i);
        auto owner_cluster_worker = partition_owner_cluster_worker(partitionId, txn->ith_replica);
        workers_need_persist_cmd_buffer[owner_cluster_worker] = true;
      }
    }
  }

  void execute_transaction_batch_haa(const std::vector<TransactionType*> all_txns,
                                 const std::vector<TransactionType*> & sp_txns, 
                                 std::vector<TransactionType*> & mp_txns) {
    last_commit = std::chrono::steady_clock::now();
    std::vector<bool> workers_need_persist_cmd_buffer(this->active_replica_worker_num_end, false);
    if (this->context.hstore_active_active) {
      DCHECK(all_txns.size() == 1);
      hstore_active_active_granules_locked.clear();
      find_partition_owners(all_txns, workers_need_persist_cmd_buffer);
    }
    int64_t txn_id = 0;
    int cnt = 0;
    uint64_t commit_persistence_us = 0;
    uint64_t commit_replication_us = 0;
    bool cmd_buffer_flushed = false;
    {
      ScopedTimer t0([&, this](uint64_t us) {
        execution_phase_time.add(us);
      });

      int cnt = 0;
      process_mp_transactions(mp_txns);
      for (size_t i = 0; i < sp_txns.size(); ++i) {
        auto txn = sp_txns[i];
        txn->reset();
        txn_id = txn->transaction_id;
        if (!process_single_transaction(txn)) {
          txn->abort_lock = true;
          this->n_abort_lock.fetch_add(1);
        }
        handle_requests();
        if (++cnt % 2 == 0 && replica_num > 1 && is_replica_worker == false) {
          if (this->context.lotus_async_repl == false) {
            send_commands_to_replica(true);
          }
        }
      }
      if (replica_num > 1 && is_replica_worker == false) {
        if (this->context.lotus_async_repl == false) {
          send_commands_to_replica(true);
        }
      }

      if (is_replica_worker == false && replica_num > 1) {
        ScopedTimer t1([&, this](uint64_t us) {
          commit_replication_us = us;
          replication_time.add(us);
        });
        int first_account = 0;
        int communication_rounds = 0;
        auto minimum_coord_txn_written_log_position_snap = minimum_coord_txn_written_log_position;
        while (minimum_replayed_log_position < minimum_coord_txn_written_log_position_snap) {
          if (this->context.lotus_async_repl) {
            break;
          }
          ScopedTimer t2([&, this](uint64_t us) {
            this->replica_progress_query_latency.add(us);
          });
          // Keep querying the replica for its replayed log position until minimum_replayed_log_position >= minimum_coord_txn_written_log_position_snap
          DCHECK(0 <= replica_cluster_worker_id && replica_cluster_worker_id < cluster_worker_num);
          cluster_worker_messages[replica_cluster_worker_id]->set_transaction_id(txn_id);
          MessageFactoryType::new_get_replayed_log_posistion_message(*cluster_worker_messages[replica_cluster_worker_id], minimum_coord_txn_written_log_position_snap, 1, this_cluster_worker_id);
          add_outgoing_message(replica_cluster_worker_id);
          flush_messages();
          get_replica_replay_log_position_requests++;
          
          while (get_replica_replay_log_position_responses < get_replica_replay_log_position_requests) {
            if (cmd_buffer_flushed == false && this->context.hstore_active_active == false) {
              ScopedTimer t0([&, this](uint64_t us) {
                commit_persistence_us = us;
                window_persistence_latency.add(us);
                this->last_window_persistence_latency.store(window_persistence_latency.average());
              });
              persist_and_clear_command_buffer(true);
              cmd_buffer_flushed = true;
            }
            handle_requests();
            if (replica_num > 1 && is_replica_worker == false) {
              if (this->context.lotus_async_repl == false) {
                send_commands_to_replica(true);
              }
            }
          }
          DCHECK(get_replica_replay_log_position_responses == get_replica_replay_log_position_requests);
          if (first_account == 0) {
            //if (minimum_replayed_log_position < minimum_coord_txn_written_log_position_snap) {
              auto gap = std::max((int64_t)0, minimum_coord_txn_written_log_position_snap - minimum_replayed_log_position);
              this->replication_gap_after_active_replica_execution.add(gap);
            //}
            first_account = 1;
          }
          communication_rounds++;
        }
        this->replication_sync_comm_rounds.add(communication_rounds);
      }
    }

    ScopedTimer t0([&, this](uint64_t us) {
      execution_after_commit_time.add(us);
    });
    {
      ScopedTimer t0([&, this](uint64_t us) {
        commit_persistence_us += us;
      });
      DCHECK((int)workers_need_persist_cmd_buffer.size() <= this->cluster_worker_num);
      for (int i = 0; i < (int)workers_need_persist_cmd_buffer.size(); ++i) {
        if (!workers_need_persist_cmd_buffer[i] || i == this_cluster_worker_id)
          continue;
        cluster_worker_messages[i]->set_transaction_id(txn_id);
        MessageFactoryType::new_persist_cmd_buffer_message(*cluster_worker_messages[i], 0, this_cluster_worker_id);
        add_outgoing_message(i);
        sent_persist_cmd_buffer_requests++;
      }
      flush_messages();
      while (received_persist_cmd_buffer_responses < sent_persist_cmd_buffer_requests) {
        handle_requests(false);
      }
    }
    if (cmd_buffer_flushed == false) {
      ScopedTimer t0([&, this](uint64_t us) {
        commit_persistence_us += us;
        window_persistence_latency.add(us);
        this->last_window_persistence_latency.store(window_persistence_latency.average());
      });
      persist_and_clear_command_buffer(true);
      cmd_buffer_flushed = true;
    }
    if (this->context.lotus_async_repl) {
      send_commands_to_replica(true);
    }
    if (this->context.hstore_active_active) {
      DCHECK(all_txns.size() == 1);
      release_locks_async(*all_txns[0], cluster_worker_messages, false, true);
    }

    auto & txns = all_txns;
    size_t committed = 0;
    for (size_t i = 0; i < txns.size(); ++i) {
      auto txn = txns[i];
      if (txn->abort_lock || txn->abort_no_retry)
        continue;
      committed++;
      txn->record_commit_persistence_time(commit_persistence_us);
      txn->record_commit_replication_time(commit_replication_us);
      //DCHECK(!txn->is_single_partition());
      auto latency =
          std::chrono::duration_cast<std::chrono::microseconds>(
              std::chrono::steady_clock::now() - txn->startTime)
              .count();
      this->percentile.add(latency);
      // window_txn_latency.add(latency);
      // this->last_window_txn_latency.store(window_txn_latency.average());
      if (txn->distributed_transaction) {
        this->dist_latency.add(latency);
      } else {
        this->local_latency.add(latency);
      }
      // Make sure it is unlocked.
      // if (txn->is_single_partition())
      //   DCHECK(lock_buckets[partition_id] == -1);
      this->record_txn_breakdown_stats(*txn);
    }
    this->round_concurrency.add(txns.size());
    this->effective_round_concurrency.add(committed);
    mp_concurrency_limit.add(mp_concurrency_max);
  }

  void post_commit_work(TransactionType* txn, uint64_t commit_persistence_us, uint64_t & committed) {
    if (txn->abort_lock || txn->abort_no_retry)
      return;
    committed++;
    {
      ScopedTimer t([&, this](uint64_t us) {
        txn->record_commit_unlock_time(us);
      });
      if (this->context.hstore_active_active) {
        release_locks_async(*txn, cluster_worker_messages, is_replica_worker == false);
      } else {
        if (txn->is_single_partition() == false)
          release_locks_async(*txn, cluster_worker_messages, is_replica_worker == false);
      }
    }
    txn->record_commit_persistence_time(commit_persistence_us);
    //DCHECK(!txn->is_single_partition());
    auto latency =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - txn->startTime)
            .count();
    this->percentile.add(latency);
    window_txn_latency.add(latency);
    this->last_window_txn_latency.store(window_txn_latency.average());
    if (txn->distributed_transaction) {
      this->dist_latency.add(latency);
    } else {
      this->local_latency.add(latency);
    }
    // Make sure it is unlocked.
    // if (txn->is_single_partition())
    //   DCHECK(lock_buckets[partition_id] == -1);
    this->record_txn_breakdown_stats(*txn);
  }

  void post_commit_work(const std::vector<TransactionType*> & txns, uint64_t commit_persistence_us, uint64_t & committed) {
    for (size_t i = 0; i < txns.size(); ++i) {
      auto txn = txns[i];
      post_commit_work(txn, commit_persistence_us, committed);
    }
  }

  void execute_transaction_batch(const std::vector<TransactionType*> all_txns,
                                 const std::vector<TransactionType*> & sp_txns, 
                                 std::vector<TransactionType*> & mp_txns) {
    last_commit = std::chrono::steady_clock::now();
    std::vector<bool> workers_need_persist_cmd_buffer(this->active_replica_worker_num_end, false);
    CHECK(this->context.lotus_async_repl);
    CHECK(!this->context.hstore_active_active);
    uint64_t commit_persistence_us = 0;
    uint64_t commit_replication_us = 0;
    uint64_t committed = 0;
    handle_requests(false);
    if (sp_txns.empty() == false) {
      undo_buffer.clear();
      ScopedTimer t0([&, this](uint64_t us) {
        execution_phase_time.add(us);
      });
      if (sp_parallel_exec_commit) {
        // Run commit in paralell with execution
        // Record lock_bm with SP transaction commands together
        for (auto txn: sp_txns) {
          txn->reset();
        }
        write_command_for_sp_group(sp_txns);
        
        std::size_t sp_idx_executed = 0;
        std::size_t sp_idx_committed = 0;
        auto execute_sp = [&, this]() {
          if (sp_idx_executed >= sp_txns.size())
            return;
          auto txn = sp_txns[sp_idx_executed];
          if (!process_single_transaction(txn)) {
            txn->abort_lock = true;
            this->n_abort_lock.fetch_add(1);
          }
          sp_idx_executed++;
        };

        auto post_commit_func = [&, this] () {
          for(; sp_idx_committed < sp_idx_executed; ++sp_idx_committed) {
            post_commit_work(sp_txns[sp_idx_committed], commit_persistence_us, committed);
          }
        };
        {
          ScopedTimer t0([&, this](uint64_t us) {
              commit_persistence_us = us;
              window_persistence_latency.add(us);
              this->last_window_persistence_latency.store(window_persistence_latency.average());
            });
          // parallelize execution with persistence
          persist_and_clear_command_buffer(execute_sp);
        }
        while (sp_idx_executed < sp_txns.size()) {
          execute_sp();
          post_commit_func();
        }
      } else {
        // RCST for single-partition transactions
        for (size_t i = 0; i < sp_txns.size(); ++i) {
          auto txn = sp_txns[i];
          txn->reset();
          if (!process_single_transaction(txn)) {
            txn->abort_lock = true;
            this->n_abort_lock.fetch_add(1);
          }
        }
        // Commit after 
        {
          ScopedTimer t0([&, this](uint64_t us) {
                commit_persistence_us = us;
                window_persistence_latency.add(us);
                this->last_window_persistence_latency.store(window_persistence_latency.average());
              });
          persist_and_clear_command_buffer(true);
        }
        send_commands_to_replica(true);
        post_commit_work(sp_txns, commit_persistence_us, committed);
        // Return results to clients
      }
    }

    {
      ScopedTimer t0([&, this](uint64_t us) {
        execution_after_commit_time.add(us);
      });
      // MEST for multi-partition transactions
      process_mp_transactions(mp_txns);
      if (mp_txns.empty() == false) {
        {
          ScopedTimer t0([&, this](uint64_t us) {
              commit_persistence_us = us;
              window_persistence_latency.add(us);
              this->last_window_persistence_latency.store(window_persistence_latency.average());
            });
          persist_and_clear_command_buffer(true);
        }
        send_commands_to_replica(true);
        post_commit_work(mp_txns, commit_persistence_us, committed);
        // Return results to clients
      }
    }

    this->round_concurrency.add(all_txns.size());
    this->effective_round_concurrency.add(committed);
    mp_concurrency_limit.add(mp_concurrency_max);
  }

  void process_batch_of_transactions() {
    if (pending_txns.empty())
      return;
    std::size_t until_ith = pending_txns.size();
    // Execution
    auto txns = std::vector<TransactionType*>(pending_txns.begin(), pending_txns.begin() + until_ith);
    std::vector<TransactionType*> sp_txns;
    std::vector<TransactionType*> mp_txns;
    {
      ScopedTimer t0([&, this](uint64_t us) {
        scheduling_time += us;
      });
      for (size_t i = 0; i < txns.size(); ++i) {
        if (txns[i]->is_single_partition()) {
          sp_txns.push_back(txns[i]);
        } else {
          mp_txns.push_back(txns[i]);
        }
      }
    }
    
    if (this->context.hstore_active_active) {
      execute_transaction_batch_haa(txns, sp_txns, mp_txns);
    } else {
      execute_transaction_batch(txns, sp_txns, mp_txns);
    }
    
    DCHECK(active_txns.empty());
    DCHECK(until_ith <= pending_txns.size());
    {
      ScopedTimer t0([&, this](uint64_t us) {
        scheduling_time += us;
      });
      for (std::size_t i = 0; i < until_ith; ++i) {
        if (pending_txns.front()->abort_lock == false || pending_txns.front()->abort_no_retry) {
          DCHECK(active_txns.count(pending_txns.front()->transaction_id) == 0);
          std::unique_ptr<TransactionType> txn(pending_txns.front());
          pending_txns.pop_front();
        } else {
          pending_txns.push_back(pending_txns.front());
          pending_txns.pop_front();
        }
      }
    }
  }

  uint64_t scheduling_time = 0;
  void process_new_transactions() {
    scheduling_time = 0;
    {
      ScopedTimer t([&, this](uint64_t us) {
          scheduling_time += us;
        });
      if (pending_txns.size() < batch_per_worker) {
        fill_pending_txns(batch_per_worker);
      }
    }
    if (pending_txns.empty()) {
      return;
    }
    process_batch_of_transactions();
    scheduling_cost.add(scheduling_time);
    return;
  }

  bool processing_mp = false;

  // TODO: replay lock_buckets
  void replay_sp_queue_commands_unprotected(std::deque<TxnCommandBase> & q) {
    while (q.empty() == false) {
      auto & cmd = q.front();
      if (cmd.is_cow_end || cmd.is_cow_begin) {
        if (cmd.is_cow_cmd_processed) {
          break;
        }
        --((ReplicaCheckpointMeta*)cmd.txn)->ref_cnt;
        cmd.is_cow_cmd_processed = true;
        auto cmd_lock_id = to_lock_id(cmd.partition_id, cmd.granule_id);
        if (((ReplicaCheckpointMeta*)cmd.txn)->ref_cnt == 0) {
          std::string tag = cmd.is_cow_begin ? "COW_BEGIN" : "COW_END";
          LOG(INFO) << "Replica worker " << this_cluster_worker_id << " " << tag << " at step " << ((ReplicaCheckpointMeta*)cmd.txn)->checkpoint_step << " started";
          if (tag == "COW_BEGIN") {
            this->db.start_checkpoint_process(managed_partitions);
          } else {
            while (this->db.checkpoint_work_finished(managed_partitions) == false);
            this->db.stop_checkpoint_process(managed_partitions);
          }
          for (auto lock_id : lock_ids_by_this_replica_executor) {
            DCHECK(get_granule_lock_cmd_queue(lock_id).empty() == false);
            DCHECK(get_granule_lock_cmd_queue(lock_id).front().txn == cmd.txn);
            DCHECK(((ReplicaCheckpointMeta*)(get_granule_lock_cmd_queue(lock_id).front().txn))->ref_cnt == 0);
            DCHECK(get_granule_lock_cmd_queue(lock_id).front().is_cow_begin || get_granule_lock_cmd_queue(lock_id).front().is_cow_end);
            get_granule_lock_cmd_queue(lock_id).pop_front();
            if (cmd_lock_id != lock_id) {
              replay_sp_queue_commands_unprotected(get_granule_lock_cmd_queue(lock_id));
            }
          }
          LOG(INFO) << "Replica worker " << this_cluster_worker_id << " " << tag << " at step " << ((ReplicaCheckpointMeta*)cmd.txn)->checkpoint_step <<" finished";
        }
        break;
      } else if (cmd.is_coordinator == false) {
        //DCHECK(false);
        auto partition_id = cmd.partition_id;
        auto granule_id = cmd.granule_id;
        auto lock_id = to_lock_id(partition_id, granule_id);
        auto & lock_bucket = get_lock_bucket(lock_id);
        DCHECK(cmd.last_writer != std::numeric_limits<uint64_t>::max());
        if (cmd.write_lock) {
          if (lock_bucket.free()) { // only acquire the lock when there is no readers and writer
            lock_bucket.set_last_writer(cmd.tid);
            lock_bucket.set_write_lock();

            if (cmd.txn != nullptr && --((TransactionType*)cmd.txn)->granules_left_to_lock == 0) {
              txns_candidate_for_reexecution.push_back((TransactionType*)cmd.txn);
            }
            DCHECK(get_partition_last_replayed_position_in_log(lock_id) <= cmd.position_in_log);
            get_partition_last_replayed_position_in_log(lock_id) = cmd.position_in_log;
            get_granule_lock_time(lock_id) = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
            q.pop_front();
            auto & q2 = get_granule_lock_request_queue(lock_id);
            if (q2.empty() == false) {
              granule_lock_reqeust_candidates.push_back(lock_id);
            }
          } else {
            break;
          }
        } else {
          if (lock_bucket.write_locked() || lock_bucket.get_last_writer() != cmd.last_writer) {
            break;
          }
          DCHECK(lock_bucket.get_last_writer() == cmd.last_writer);
          lock_bucket.inc_reader_cnt();
          if (cmd.txn != nullptr && --((TransactionType*)cmd.txn)->granules_left_to_lock == 0) {
            txns_candidate_for_reexecution.push_back((TransactionType*)cmd.txn);
          }
          //LOG(INFO) << "This cluster worker " << this_cluster_worker_id << " replayed participant read lock record of txn " << cmd.tid << " on lock_id " << lock_id << " with last writer " << cmd.last_writer;
          DCHECK(get_partition_last_replayed_position_in_log(lock_id) <= cmd.position_in_log);
          get_partition_last_replayed_position_in_log(lock_id) = cmd.position_in_log;
          get_granule_lock_time(lock_id) = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
          q.pop_front();
          auto & q2 = get_granule_lock_request_queue(lock_id);
          if (q2.empty() == false) {
            granule_lock_reqeust_candidates.push_back(lock_id);
          }
        }
      } else if (cmd.is_mp == false) {
        DCHECK(false);
        // DCHECK(cmd.txn != nullptr);
        // // if (cmd.txn->being_replayed == false) {
        // //   auto queue_time =
        // //   std::chrono::duration_cast<std::chrono::microseconds>(
        // //       std::chrono::steady_clock::now() - cmd.txn->startTime)
        // //       .count();
        // //   cmd.txn->being_replayed = true;
        // //   cmd.txn->startTime = std::chrono::steady_clock::now();
        // //   cmd.txn->record_commit_prepare_time(queue_time);
        // // }
        // auto partition_id = cmd.partition_id;
        // auto granule_id = cmd.granule_id;
        // auto lock_id = to_lock_id(partition_id, granule_id);
        // if (lock_bucket == -1) {
        //   // The transaction owns the partition.
        //   // Start executing the sp transaction.
        //   auto sp_txn = cmd.txn;
        //   DCHECK(get_partition_last_replayed_position_in_log(lock_id) <= cmd.position_in_log);
        //   get_partition_last_replayed_position_in_log(lock_id) = cmd.position_in_log;
        //   lock_bucket = sp_txn->transaction_id;
        //   lock_bm.set_bit(lock_id);
        //   txns_candidate_for_reexecution.push_back(sp_txn);
        //   q.pop_front();
        //   // DCHECK(sp_txn->transaction_id);
        //   // DCHECK(sp_txn);
        //   // auto res = process_single_transaction(sp_txn);
        //   // DCHECK(res);
        //   // if (res) {
        //   //   auto latency =
        //   //   std::chrono::duration_cast<std::chrono::microseconds>(
        //   //       std::chrono::steady_clock::now() - sp_txn->startTime)
        //   //       .count();
        //   //   this->percentile.add(latency);
        //   //   this->local_latency.add(latency);
        //   //   this->record_txn_breakdown_stats(*sp_txn);
        //   //   DCHECK(get_partition_last_replayed_position_in_log(lock_id) <= cmd.position_in_log);
        //   //   get_partition_last_replayed_position_in_log(lock_id) = cmd.position_in_log;
        //   //   respond_to_active_replica_with_replay_position(cmd.position_in_log);
        //   //   // Make sure it is unlocked
        //   //   DCHECK(lock_bucket == -1);
        //   //   delete sp_txn;
        //   //   q.pop_front();
        //   // } else {
        //   //   // Make sure it is unlocked
        //   //   DCHECK(lock_bucket == -1);
        //   // }
        // } else {
        //   // The partition is being locked by front transaction executing, try next time.
        //   break;
        // }
      } else {
        break;
      }
    }
  }
// TODO: replay lock_buckets
  bool replay_sp_queue_commands(int queue_index) {
    int i = queue_index;
    if (granule_command_queue_processing[i])
        return false;
    auto & q = granule_command_queues[i];
    if (q.empty())
      return false;
    granule_command_queue_processing[i] = true;
    replay_sp_queue_commands_unprotected(q);
    granule_command_queue_processing[i] = false;
    return true;
  }

  bool replay_sp_commands(int partition) {
    // TODO: replay lock_buckets
    return replay_sp_queue_commands(granule_to_cmd_queue_index[partition]);
  }

  uint64_t cross_node_mp_txn = 0;
  uint64_t cross_worker_mp_txn = 0;
  uint64_t single_worker_mp_txn = 0;
  int active_mps = 0;
  int delete_mp_txns = 0;
  int txns_replayed = 0;
  int active_mp_limit = 0;
  void replay_commands_in_granule(int lock_id) {
    //TODO: replay lock_buckets
    int i = granule_to_cmd_queue_index[lock_id];
    if (granule_command_queue_processing[i])
        return;
    replay_sp_queue_commands(i);
    return; 
  }

  std::vector<TransactionType*> tmp_txns_candidate_for_reexecution;
  void merge_replay_points() {
    if (replay_points.size() <= 1)
      return;
    
    if (replay_points[0].second + replay_points[1].second + (int)active_txns.size() < active_mp_limit) {
      int new_count = replay_points[0].second + replay_points[1].second;
      replay_points.pop_front();
      replay_points[0].second = new_count;
    }
  }

  void replay_loop() {
    // if (std::chrono::duration_cast<std::chrono::milliseconds>(
    //         std::chrono::steady_clock::now() - executor_start_time)
    //         .count() <= 2000) {
    //   return;
    // }
    auto complete_mp = [&, this](TransactionType* mp_txn) {
      DCHECK(active_txns.count(mp_txn->transaction_id) == 0);
      //LOG(INFO) << "This cluster worker " << this_cluster_worker_id << " completes MP transaction " << mp_txn->transaction_id;
      //DCHECK(mp_txn->is_single_partition() == false);
      auto lock_id = mp_txn->replay_queue_lock_id;
      auto replay_queue_idx = granule_to_cmd_queue_index[mp_txn->replay_queue_lock_id];
      DCHECK(granule_command_queue_processing[replay_queue_idx] == false);
      // CHECK(replay_points.empty() == false);
      // CHECK(replay_points.front().first >= mp_txn->position_in_log);
      // CHECK(replay_points.front().second > 0);
      // --replay_points.front().second;
      if (mp_txn->finished_commit_phase && (mp_txn->abort_lock == false || mp_txn->abort_no_retry)) {
        DCHECK(mp_txn->release_lock_called);
        auto latency =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - mp_txn->startTime)
            .count();
        this->percentile.add(latency);
        window_txn_latency.add(latency);
        this->last_window_txn_latency.store(window_txn_latency.average());
        this->dist_latency.add(latency);
        DCHECK(mp_txn->tries == 1);
        this->record_txn_breakdown_stats(*mp_txn);
        respond_to_active_replica_with_replay_position(mp_txn->position_in_log);
        //granule_command_queue_processing[replay_queue_idx] = false;
        tries_latency[std::min((int)mp_txn->tries, (int)tries_latency.size() - 1)].add(latency);
        if (mp_txn->replay_bm_for_sp) {
          mp_txn->replay_bm_for_sp->dec_ref();
        }
        delete mp_txn;
        delete_mp_txns++;
      } else {
        CHECK(false);
      }

      --active_mps;
    };
    window_active_mps.add(active_mps);
    this->last_window_active_txns.store(window_active_mps.average());
    process_to_commit(async_txns_to_commit, complete_mp);
    decode_replay_buffer();
    process_queued_lock_requests();
    tmp_txns_candidate_for_reexecution.clear();
    //if (active_mps < active_mp_limit) {
      while (txns_candidate_for_reexecution.empty() == false) {
        auto txn = txns_candidate_for_reexecution.front();
        txns_candidate_for_reexecution.pop_front();
        // CHECK(replay_points.empty() == false);
        // if (replay_points.front().second == 0) {
        //   DCHECK(replay_points.size() > 1);
        //   replay_points.pop_front();
        //   merge_replay_points();
        // }
        process_queued_lock_requests();
        decode_replay_buffer();
        // if (txn->position_in_log > replay_points.front().first) {
        //   //merge_replay_points();
        //   tmp_txns_candidate_for_reexecution.push_back(txn);
        //   continue;
        // }
        
        txn->startTime = std::chrono::steady_clock::now();
        auto replay_queue_idx = granule_to_cmd_queue_index[txn->replay_queue_lock_id];
        DCHECK(granule_command_queue_processing[replay_queue_idx] == false);
        //granule_command_queue_processing[replay_queue_idx] = true;
        txn->tries++;
        ++active_mps;
        if (txn->is_single_partition()) {
          DCHECK(txn->lock_status.num_locks() > 0);
          if (sp_parallel_exec_commit) {
            CHECK(txn->replay_bm_for_sp != nullptr);
          }
          auto res = process_single_transaction(txn, true);
          if (sp_parallel_exec_commit && txn->abort_lock_bm == true) {
            CHECK(res == false);
            release_locks_async(*txn, cluster_worker_messages, false);
          } else {
            auto latency =
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - txn->startTime)
                .count();
            window_txn_latency.add(latency);
            this->last_window_txn_latency.store(window_txn_latency.average());
            this->percentile.add(latency);
            this->local_latency.add(latency);
            this->record_txn_breakdown_stats(*txn);
          }
          // CHECK(replay_points.empty() == false);
          // CHECK(replay_points.front().first >= txn->position_in_log);
          // CHECK(replay_points.front().second > 0);
          // --replay_points.front().second;
          respond_to_active_replica_with_replay_position(txn->position_in_log);
          // Make sure it is unlocked
          delete txn;
          delete_mp_txns++;
          //granule_command_queue_processing[replay_queue_idx] = false;
          --active_mps;
        } else {
          //execute_transaction(txn);
          process_execution_async_single_mp_txn(txn);
          if (async_txns_to_commit.empty() == false) {
            process_to_commit(async_txns_to_commit, complete_mp);
            //merge_replay_points();
          }
        }
      }
      // for (auto txn : tmp_txns_candidate_for_reexecution) {
      //   txns_candidate_for_reexecution.push_back(txn);
      // }
    //}

    handle_requests(false);
    merge_replay_points();
    process_to_commit(async_txns_to_commit, complete_mp);
    if (active_mps == 0 && txns_candidate_for_reexecution.empty() && async_txns_to_commit.empty() && active_replica_waiting_position != -1) {
      respond_to_active_replica_with_replay_position(get_minimum_replayed_log_position());
    }
    //replay_mp_concurrency.add(active_mps);
  }

  std::string straggler_mp_debug_string;
  int straggler_count = 0;

  void send_commands_to_replica(bool persist = false) {
    if (replica_num <= 1 || command_buffer_outgoing_data.empty())
      return; // Nothing to send
    //auto data = serialize_commands(command_buffer_outgoing.begin(), command_buffer_outgoing.end());
    DCHECK(0 <= replica_cluster_worker_id && replica_cluster_worker_id < cluster_worker_num);
    MessageFactoryType::new_command_replication(
            *cluster_worker_messages[replica_cluster_worker_id], 1, command_buffer_outgoing_data, this_cluster_worker_id, persist);
    add_outgoing_message(replica_cluster_worker_id);
    flush_messages();
    //LOG(INFO) << "This cluster worker " << this_cluster_worker_id << " sent " << command_buffer_outgoing.size() << " commands to replia worker " <<  replica_cluster_worker_id;
    command_buffer_outgoing_data.clear();
  }

  void push_replica_message(Message *message) override {
    DCHECK(is_replica_worker == true);
    DCHECK(message->get_is_replica());
    this->push_message(message);
  }

  std::deque<TransactionType*> async_txns_to_commit;

  void process_queue_partition_lock_request(int lock_id) {
    auto & q = get_granule_lock_request_queue(lock_id);
    if (q.empty())
      return;
    
    bool reorg = false;
    for (std::size_t i = 0; i < q.size(); ++i) {
      auto & messagePiece = q[i];
      auto message = messagePiece.message_ptr;
      DCHECK(message);
      bool good = true;
      auto type = messagePiece.get_message_type();
      DCHECK(type == (int)HStoreMessage::ACQUIRE_LOCK_AND_READ_REQUEST);
      auto lock_id_2 = to_lock_id(messagePiece.get_partition_id(), messagePiece.get_granule_id());
      DCHECK(lock_id_2 == lock_id);

      ITable *table = this->db.find_table(messagePiece.get_table_id(),
                                    messagePiece.get_partition_id());
      TransactionType * txn = nullptr;
      bool res = acquire_lock_and_read_request_handler(*message, messagePiece,
                                                *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                                txn);
      if (res == true) {
        window_queued_lock_req_latency.add(std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()).time_since_epoch().count() - message->get_message_recv_time());
        this->last_window_queued_lock_req_latency.store(window_queued_lock_req_latency.average());
        auto time_to_lock = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count() - get_granule_lock_time(lock_id);
        window_lock_req_latency.add(time_to_lock);
        this->last_window_lock_req_latency.store(window_lock_req_latency.average());
        add_outgoing_message(message->get_source_cluster_worker_id());
        if (--message->ref_cnt == 0) {
          std::unique_ptr<Message> rel(message);
        }
        reorg = true;
        messagePiece.message_ptr = nullptr;
        flush_messages();
      } else {
        DCHECK(message->ref_cnt > 0);
        cluster_worker_messages[message->get_source_cluster_worker_id()]->clear_message_pieces();
      }
    }
    if (reorg) {
      std::size_t j = 0;
      for (std::size_t i = 0; i < q.size(); ++i) {
        if (q[i].message_ptr != nullptr) {
          q[j++] = q[i];
        }
      }
      q.resize(j);
    }
  }

  void process_queued_lock_requests() {
    if (granule_lock_reqeust_candidates.empty())
      return;
    // ScopedTimer t([&, this](uint64_t us) {
    //   handle_latency.add(us);
    // });
    auto sz = granule_lock_reqeust_candidates.size();
    for (size_t i = 0; i < sz; ++i) {
      auto lock_id = granule_lock_reqeust_candidates[i];
      process_queue_partition_lock_request(lock_id);
    }
    while (sz--) {
      granule_lock_reqeust_candidates.pop_front();
    }
  }

  std::vector<MessagePiece> failed_lock_requests;
  std::size_t handle_requests(bool should_replay_commands = true) {
    if (this->in_queue.empty())
      return 0;

    TransactionType * txn = nullptr;
    int msg_count = 0;
    std::size_t size = 0;
    while (!this->in_queue.empty()) {
      failed_lock_requests.clear();
      ++size;
      std::unique_ptr<Message> message(this->in_queue.front());
      bool ok = this->in_queue.pop();
      CHECK(ok);
      DCHECK(message->get_worker_id() == this->id);
      if (message->get_is_replica())
        DCHECK(is_replica_worker);
      else
        DCHECK(!is_replica_worker);
//       int message_type = 0;

      int msg_idx = 0;
      bool acquire_partition_lock_requests_successful = true;
      auto msg_cnt = message->get_message_count();
      bool replication_command = false;
      
      //message->set_message_recv_time(std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()).time_since_epoch().count());

      {
      // ScopedTimer t([&, this](uint64_t us) {
      //   //message_processing_latency[message_type].add(us);
      //   if (message_type == (int)HStoreMessage::ACQUIRE_LOCK_AND_READ_REQUEST
      //    ) {
      //     DCHECK(message->get_message_gen_time() != 0);
      //     DCHECK(message->get_message_gen_time() <= message->get_message_send_time());
      //     DCHECK(message->get_message_gen_time() <= message->get_message_recv_time());
      //     msg_send_latency.add(message->get_message_send_time() - message->get_message_gen_time());
      //     msg_recv_latency.add(message->get_message_recv_time() - message->get_message_gen_time());
      //     msg_proc_latency.add(std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()).time_since_epoch().count() - message->get_message_gen_time());
      //     //msg_recv_latency.add(std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::steady_clock::now()).time_since_epoch().count() - message->get_message_recv_time());
      //   }
      // });
      auto tid = message->get_transaction_id();
      if (txn == nullptr || txn->transaction_id != tid) {
        txn = nullptr;
        if (active_txns.count(tid) > 0) {
          txn = active_txns[tid];
        }
      }
      for (auto it = message->begin(); it != message->end(); it++, ++msg_idx) {
        msg_count++;
        MessagePiece messagePiece = *it;
        auto type = messagePiece.get_message_type();
        // message_type = type;
        //LOG(INFO) << "Message type " << type;
        // auto message_partition_owner_cluster_worker_id = partition_owner_cluster_worker(message_partition_id);
        
        // if (type != (int)HStoreMessage::MASTER_UNLOCK_PARTITION_RESPONSE && type != (int)HStoreMessage::MASTER_LOCK_PARTITION_RESPONSE
        //     && type != (int)HStoreMessage::ACQUIRE_PARTITION_LOCK_AND_READ_RESPONSE && type != (int) HStoreMessage::WRITE_BACK_RESPONSE && type != (int)HStoreMessage::RELEASE_READ_LOCK_RESPONSE
        //     && type != (int)HStoreMessage::RELEASE_PARTITION_LOCK_RESPONSE && type != (int)HStoreMessage::PREPARE_REQUEST && type != (int)HStoreMessage::PREPARE_RESPONSE && type != (int)HStoreMessage::PREPARE_REDO_REQUEST && type != (int)HStoreMessage::PREPARE_REDO_RESPONSE) {
        //   CHECK(message_partition_owner_cluster_worker_id == this_cluster_worker_id);
        // }
        ITable *table = this->db.find_table(messagePiece.get_table_id(),
                                      messagePiece.get_partition_id());
//        DCHECK(message->get_source_cluster_worker_id() != this_cluster_worker_id);
        //DCHECK(message->get_source_cluster_worker_id() < (int32_t)this->context.partition_num);

        if (type == (int)HStoreMessage::ACQUIRE_LOCK_AND_READ_REQUEST) {
          bool res = acquire_lock_and_read_request_handler(*message, messagePiece,
                                                 *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                                 txn);
          if (res == false) {
            if (is_replica_worker) {
              failed_lock_requests.emplace_back(messagePiece);
            }
            acquire_partition_lock_requests_successful = false;
          }
        } else if (type == (int)HStoreMessage::ACQUIRE_LOCK_AND_READ_RESPONSE) {
          acquire_lock_and_read_response_handler(*message, messagePiece,
                                                 *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                                 txn);
          DCHECK(txn);
          if (txn->pendingResponses == 0) {
            async_txns_to_commit.push_back(txn);
            if (txn->abort_lock == false) {
              // auto ltc = std::chrono::duration_cast<std::chrono::microseconds>(
              // std::chrono::steady_clock::now() - txn->lock_issue_time)
              // .count();
              //tries_lock_response_latency[std::min((int)txn->tries, (int)tries_lock_response_latency.size() - 1)].add(ltc);
            }
          }
        } else if (type == (int)HStoreMessage::WRITE_BACK_REQUEST) {
          write_back_request_handler(*message, messagePiece,
                                    *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                    txn);
        } else if (type == (int)HStoreMessage::WRITE_BACK_RESPONSE) {
          write_back_response_handler(*message, messagePiece,
                                      *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                      txn);
        } else if (type == (int)HStoreMessage::RELEASE_LOCK_REQUEST) {
          release_lock_request_handler(*message, messagePiece,
                                                 *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                                 txn);
        } else if (type == (int)HStoreMessage::RELEASE_LOCK_RESPONSE) {
          release_lock_response_handler(*message, messagePiece,
                                                 *cluster_worker_messages[message->get_source_cluster_worker_id()], *table,
                                                 txn);
        } else if (type == (int)HStoreMessage::COMMAND_REPLICATION_REQUEST) {
          command_replication_request_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()], 
                                                *table, 
                                                txn);
          replication_command = true;
        } else if (type == (int)HStoreMessage::COMMAND_REPLICATION_RESPONSE) {
          command_replication_response_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()], 
                                                *table, 
                                                txn);
        } else if (type == (int)HStoreMessage::PERSIST_CMD_BUFFER_REQUEST) {
          persist_cmd_buffer_request_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()], 
                                                *table, 
                                                txn);
        } else if (type == (int)HStoreMessage::PERSIST_CMD_BUFFER_RESPONSE) {
          persist_cmd_buffer_response_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()], 
                                                *table, 
                                                txn);
        } else if (type == (int)HStoreMessage::GET_REPLAYED_LOG_POSITION_REQUEST) {
          get_replayed_log_position_request_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()], 
                                                *table, 
                                                txn);
        } else if (type == (int)HStoreMessage::GET_REPLAYED_LOG_POSITION_RESPONSE) {
          get_replayed_log_position_response_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()], 
                                                *table, 
                                                txn);
        } else if (type == (int)HStoreMessage::RTT_REQUEST) {
          rtt_test_request_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()], 
                                                *table, 
                                                txn);
        } else if (type == (int)HStoreMessage::RTT_RESPONSE) {
          DCHECK(rtt_request_sent);
          auto rtt = std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now() - rtt_request_sent_time)
          .count(); 
          rtt_request_sent = false;
          rtt_stat.add(rtt);
        } else if (type == (int)HStoreMessage::CHECKPOINT_INST) { 
          checkpoint_inst_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()], 
                                                *table, 
                                                txn);
        } else if (type == (int)HStoreMessage::WRITE_PARTICIPANT_COMMAND_REQUEST) {
          write_participant_command_request_handler(*message, messagePiece, *cluster_worker_messages[message->get_source_cluster_worker_id()], 
                                                *table, 
                                                txn);
        }else {
          CHECK(false);
        }

        this->message_stats[type]++;
        this->message_sizes[type] += messagePiece.get_message_length();
      }
      add_outgoing_message(message->get_source_cluster_worker_id());
      }
      if (is_replica_worker && acquire_partition_lock_requests_successful == false) {
        //DCHECK(false);
        //cluster_worker_messages[message->get_source_cluster_worker_id()]->clear_message_pieces();
        // Save the requests for now

        DCHECK(message->ref_cnt == 0);
        DCHECK(failed_lock_requests.size() > 0);
        for (size_t i = 0; i < failed_lock_requests.size(); ++i) {
          MessagePiece messagePiece = failed_lock_requests[i];
          auto lock_id = to_lock_id(messagePiece.get_partition_id(), messagePiece.get_granule_id());
          messagePiece.message_ptr = message.get();
          message->ref_cnt++;
          get_granule_lock_request_queue(lock_id).push_back(messagePiece);
          granule_lock_reqeust_candidates.push_back(lock_id);
        }

        DCHECK(message->ref_cnt > 0);
        flush_messages();
        message.release();
      } else {
        size += message->get_message_count();
        flush_messages();
      }

      if (replication_command) {
        break;
      }
    }

    // if (rtt_request_sent == false && ++rtt_cnt % 1000 == 0) {
    //   rtt_request_sent_time = std::chrono::steady_clock::now();
    //   cluster_worker_messages[rtt_test_target_cluster_worker]->set_transaction_id(0);
    //   HStoreMessageFactory::new_rtt_message(*cluster_worker_messages[rtt_test_target_cluster_worker], is_replica_worker, this_cluster_worker_id);
    //   flush_messages();
    //   rtt_request_sent = true;
    // }
    if (is_replica_worker == false && replica_num > 1 && command_buffer_outgoing_data.empty() == false) {
      if (this->context.lotus_async_repl == false) {
        send_commands_to_replica(true);
      }
    }
    if (is_replica_worker) {
      process_queued_lock_requests();
    }
    return size;
  }

  int rtt_cnt = 0;
  std::chrono::steady_clock::time_point rtt_request_sent_time;
  bool rtt_request_sent = false;

  std::chrono::steady_clock::time_point last_checkpoint_time = std::chrono::steady_clock::now();
  std::chrono::steady_clock::time_point last_cow_dump_start_time = std::chrono::steady_clock::now();
  bool time_to_checkpoint() {
    if (this->context.lotus_checkpoint == LotusCheckpointScheme::COW_ON_CHECKPOINT_OFF_LOGGING_ON || this->context.lotus_checkpoint == COW_OFF_CHECKPOINT_OFF_LOGGING_ON)
      return false ;
    auto now = std::chrono::steady_clock::now();
    if (std::chrono::duration_cast<std::chrono::seconds>(now - last_checkpoint_time).count() >= 10) {
      return true;
    }
    return false;
  }

  struct CheckpointInstruction {
    int source_cluster_id;
    CheckpointAction action;
  };
  std::deque<CheckpointInstruction> checkpoint_insts;

  void checkpoint_inst_handler(const Message & inputMessage, MessagePiece inputPiece,
                                   Message &responseMessage,
                                   ITable &table, Transaction *txn) {
    DCHECK(inputPiece.get_message_type() ==
           static_cast<uint32_t>(HStoreMessage::CHECKPOINT_INST));

    auto key_size = table.key_size();
    auto value_size = table.value_size();
    /*
     * The structure of a write command replication request: (ith_replica, txn data)
     * The structure of a write lock response: (success?, key offset, value?)
     */
    int ith_replica;
    int from_cluster_worker_id;
    auto stringPiece = inputPiece.toStringPiece();
    int instrcution;
    DCHECK(inputPiece.get_message_length() ==
             MessagePiece::get_header_size() + sizeof(from_cluster_worker_id) + sizeof(ith_replica) + sizeof(instrcution));
    Decoder dec(stringPiece);
    dec >> from_cluster_worker_id >> ith_replica >> instrcution;

    checkpoint_insts.push_back({from_cluster_worker_id, (CheckpointAction)instrcution});
  }
  
  int checkpointPendingResponses = 0;

  void send_to_checkpoint_coordiantor(CheckpointAction action) {
    cluster_worker_messages[0]->set_transaction_id(0);
    MessageFactoryType::new_checkpoint_instruction_message(*cluster_worker_messages[0], (int)action, 0, this_cluster_worker_id);
    add_outgoing_message(0);
    flush_messages();
  }


  void send_to_checkpoint_participants(CheckpointAction action, bool need_response) {
    for (int i = 1; i < active_replica_worker_num_end; ++i) {
      cluster_worker_messages[i]->set_transaction_id(0);
      MessageFactoryType::new_checkpoint_instruction_message(*cluster_worker_messages[i], (int)action, 0, this_cluster_worker_id);
      if (need_response) {
        checkpointPendingResponses++;
      }
      add_outgoing_message(i);
      flush_messages();
    }
  }

  enum class CheckpointCowOperation {
    BEGIN,
    END
  };

  void log_cow_operation(CheckpointCowOperation op) {
      std::string txn_command_data = "";
      int64_t tid = -1;
      bool is_mp = false;
      int partition_id = -1;
      int granule_id = -1;
      bool is_coordinator = true;
      int64_t position_in_log = next_position_in_command_log++;
      uint64_t last_writer = 0;
      bool write_lock = false;
      bool is_cow_begin = op == CheckpointCowOperation::BEGIN;
      bool is_cow_end = op == CheckpointCowOperation::END;
      minimum_coord_txn_written_log_position = position_in_log;

      size_t command_buffer_data_size = command_buffer_data.size();
      Encoder enc(command_buffer_data);
      enc << tid;
      enc << is_coordinator;
      enc << is_mp;
      enc << position_in_log;
      enc << partition_id;
      enc << granule_id;
      enc << last_writer;
      enc << write_lock;
      enc << is_cow_begin;
      enc << is_cow_end;
      enc << txn_command_data.size();
      enc.write_n_bytes(txn_command_data.data(), txn_command_data.size());
      
      if (this->context.lotus_async_repl == true) {
        command_buffer_outgoing_data.insert(command_buffer_outgoing_data.end(), command_buffer_data.begin() + command_buffer_data_size, command_buffer_data.end());
      }
      persist_and_clear_command_buffer(false);
      if (this->context.lotus_async_repl == true) {
        send_commands_to_replica();
      }
  }

  void process_checkpoint_instructions() {
    while (checkpoint_insts.empty() == false) {
      auto inst = checkpoint_insts.front();
      checkpoint_insts.pop_front();
      auto action = inst.action;
      if (this_cluster_worker_id == 0) { // checkpoint coordinator
        if (stage == ExecutorStage::CHECKPOINT_START) {
          CHECK(inst.source_cluster_id != this_cluster_worker_id);
          CHECK(action == CheckpointAction::CHECKPOINT_START_RESP);
          CHECK(checkpointPendingResponses > 0);
          LOG(INFO) << "Checkpoint coordinator worker[stage=" << to_string(stage) << "]  " << this->this_cluster_worker_id << " received a CHECKPOINT_START_RESP from " << inst.source_cluster_id << ", checkpointPendingResponses " << checkpointPendingResponses;
          if (--checkpointPendingResponses == 0) {
            LOG(INFO) << "Checkpoint coordinator worker[stage=" << to_string(stage) << "]  swtich to " << to_string(ExecutorStage::CHECKPOINT_COW);
            stage = ExecutorStage::CHECKPOINT_COW;
            //TODO: log cow operation
            last_cow_dump_start_time = std::chrono::steady_clock::now();
            send_to_checkpoint_participants(CheckpointAction::CHECKPOINT_COW, true);
            log_cow_operation(CheckpointCowOperation::BEGIN);
            this->db.start_checkpoint_process(managed_partitions);
            checkpointPendingResponses += 1; // Need to wait for this worker too.
            CHECK((int)this->context.worker_num * (int)this->partitioner->num_coordinator_for_one_replica() == checkpointPendingResponses);
          }
        } else if (stage == ExecutorStage::CHECKPOINT_COW || stage == ExecutorStage::CHECKPOINT_COW_DUMP_FINISHED) {
          CHECK(action == CheckpointAction::CHECKPOINT_COW_DUMP_FINISHED);
          CHECK(checkpointPendingResponses > 0);
          LOG(INFO) << "Checkpoint coordinator worker[stage=" << to_string(stage) << "]  " << this->this_cluster_worker_id << " received a CHECKPOINT_COW_DUMP_FINISHED from " << inst.source_cluster_id << ", checkpointPendingResponses " << checkpointPendingResponses;
          if (--checkpointPendingResponses == 0) { // All workers finish dumping data
            LOG(INFO) << "Checkpoint coordinator worker[stage=" << to_string(stage) << "]  swtich to " << to_string(ExecutorStage::CHECKPOINT_DONE);
            stage = ExecutorStage::CHECKPOINT_DONE;
            send_to_checkpoint_participants(CheckpointAction::CHECKPOINT_DONE_REQUEST, true);
            CHECK((int)this->context.worker_num * (int)this->partitioner->num_coordinator_for_one_replica() - 1 == checkpointPendingResponses);
          }
        } else if (stage == ExecutorStage::CHECKPOINT_DONE) {
          CHECK(action == CheckpointAction::CHECKPOINT_DONE_RESP);
          CHECK(checkpointPendingResponses > 0);
          LOG(INFO) << "Checkpoint coordinator worker[stage=" << to_string(stage) << "]  " << this->this_cluster_worker_id << " received a CHECKPOINT_DONE_RESP from " << inst.source_cluster_id << ", checkpointPendingResponses " << checkpointPendingResponses;
          if (--checkpointPendingResponses == 0) { // All workers synchornized, switch to normal mode
            LOG(INFO) << "Checkpoint coordinator worker[stage=" << to_string(stage) << "]  swtich to " << to_string(ExecutorStage::NORMAL);
            stage = ExecutorStage::NORMAL;
            last_checkpoint_time = std::chrono::steady_clock::now();
            //TODO: log switch operation
            send_to_checkpoint_participants(CheckpointAction::SWITCH_TO_NORMAL, false);
            log_cow_operation(CheckpointCowOperation::END);
            this->db.stop_checkpoint_process(managed_partitions);
          }
        }
      } else { // checkpoint participants
        if (stage == ExecutorStage::NORMAL) {
          CHECK(action == CheckpointAction::CHECKPOINT_START_REQUEST);
          LOG(INFO) << "Checkpoint participant worker[stage=" << to_string(stage) << "]  swtich to " << to_string(ExecutorStage::CHECKPOINT_START);
          // No residual transactions
          stage = ExecutorStage::CHECKPOINT_START;
          // Send a CheckpointAction::CHECKPOINT_START_RESP message to coordinator saying we are ready to switch to COW
          send_to_checkpoint_coordiantor(CheckpointAction::CHECKPOINT_START_RESP);
        } else if (stage == ExecutorStage::CHECKPOINT_START) {
          CHECK(action == CheckpointAction::CHECKPOINT_COW);
          // TODO: switch to COW mode and log the cow operation.
          LOG(INFO) << "Checkpoint participant worker[stage=" << to_string(stage) << "]  swtich to " << to_string(ExecutorStage::CHECKPOINT_COW);
          last_cow_dump_start_time = std::chrono::steady_clock::now();
          stage = ExecutorStage::CHECKPOINT_COW;
          log_cow_operation(CheckpointCowOperation::BEGIN);
          this->db.start_checkpoint_process(managed_partitions);
        } else if (stage == ExecutorStage::CHECKPOINT_COW) {
          // Coordiantor instructed this node to be ready for going back to normal mode.
          // This happens after all the background dumping workers finishes their work.
          CHECK(action == CheckpointAction::CHECKPOINT_COW_DUMP_FINISHED);
          LOG(INFO) << "Checkpoint participant worker[stage=" << to_string(stage) << "]  swtich to " << to_string(ExecutorStage::CHECKPOINT_COW_DUMP_FINISHED);
          stage = ExecutorStage::CHECKPOINT_COW_DUMP_FINISHED;
          send_to_checkpoint_coordiantor(CheckpointAction::CHECKPOINT_COW_DUMP_FINISHED);
        } else if (stage == ExecutorStage::CHECKPOINT_COW_DUMP_FINISHED) {
          CHECK(action == CheckpointAction::CHECKPOINT_DONE_REQUEST);
          LOG(INFO) << "Checkpoint participant worker[stage=" << to_string(stage) << "]  swtich to " << to_string(ExecutorStage::CHECKPOINT_DONE);
          stage = ExecutorStage::CHECKPOINT_DONE;
          send_to_checkpoint_coordiantor(CheckpointAction::CHECKPOINT_DONE_RESP);
        } else if (stage == ExecutorStage::CHECKPOINT_DONE) {
          // TODO: Coordiantor instructed this node to switch to normal operation.
          // Also log the switch operation.
          LOG(INFO) << "Checkpoint participant worker[stage=" << to_string(stage) << "]  swtich to " << to_string(ExecutorStage::NORMAL);
          CHECK(action == CheckpointAction::SWITCH_TO_NORMAL);
          stage = ExecutorStage::NORMAL;
          log_cow_operation(CheckpointCowOperation::END);
          this->db.stop_checkpoint_process(managed_partitions);
        }
      }
    }
  }

  void drive_event_loop(bool new_transaction = true) {
    handle_requests(false);
    if (is_replica_worker == false) {
      if (stage == ExecutorStage::NORMAL) {
        if (new_transaction && is_replica_worker == false) {
          process_new_transactions();
        }
        if (this_cluster_worker_id == 0 && this->context.lotus_checkpoint &&  time_to_checkpoint()) {
          LOG(INFO) << "Checkpoint coordinator worker[stage=" << to_string(stage) << "]  swtich to " << to_string(ExecutorStage::CHECKPOINT_START);
          stage = ExecutorStage::CHECKPOINT_START;
          last_checkpoint_time = std::chrono::steady_clock::now();
          send_to_checkpoint_participants(CheckpointAction::CHECKPOINT_START_REQUEST, true);
          CHECK((int)this->context.worker_num * (int)this->partitioner->num_coordinator_for_one_replica() - 1 == checkpointPendingResponses);
        }
      } else if (stage == ExecutorStage::CHECKPOINT_COW) {
        if (new_transaction && is_replica_worker == false) {
          process_new_transactions();
        }
        if (this->db.checkpoint_work_finished(managed_partitions)) {
          if (this_cluster_worker_id == 0) {
            LOG(INFO) << "Checkpoint coordinator worker[stage=" << to_string(stage) << "]  finished COW dump, switching to CHECKPOINT_COW_DUMP_FINISHED stage";
            stage = ExecutorStage::CHECKPOINT_COW_DUMP_FINISHED;
          }
          checkpoint_insts.push_back({this_cluster_worker_id, CheckpointAction::CHECKPOINT_COW_DUMP_FINISHED});
        }
      } else if (stage == ExecutorStage::CHECKPOINT_COW_DUMP_FINISHED) {
        if (new_transaction && is_replica_worker == false) {
          process_new_transactions();
        }
      } else {
        // Do not initiate new transactions
      }
      process_checkpoint_instructions();
    } else {
      if (replica_num > 1) {
        replay_loop();
      }
    }

    if (replica_num > 1 && is_replica_worker == false && command_buffer_outgoing_data.empty() == false) {
      if (this->context.lotus_async_repl == true) {
        send_commands_to_replica();
      }
    }
  }
  
  virtual void push_master_special_message(Message *message) override {     
  }

  virtual void push_master_message(Message *message) override { 
    
  }

  Message* pop_message_internal(LockfreeQueue<Message *> & queue) {
    if (queue.empty())
      return nullptr;

    Message *message = queue.front();

    // if (this->delay->delay_enabled()) {
    //   auto now = std::chrono::steady_clock::now();
    //   if (std::chrono::duration_cast<std::chrono::microseconds>(now -
    //                                                             message->time)
    //           .count() < this->delay->message_delay()) {
    //     return nullptr;
    //   }
    // }

    bool ok = queue.pop();
    CHECK(ok);

    return message;
  }

  int out_queue_round = 0;
  Message *pop_message() override {
    return pop_message_internal(this->out_queue);;
  }

  TransactionType * get_next_transaction() {
    if (is_replica_worker) {
      // Sleep for a while to save cpu
      //std::this_thread::sleep_for(std::chrono::microseconds(10));
      return nullptr;
    } else {
      auto partition_id = managed_partitions[this->random.next() % managed_partitions.size()];
      auto granule_id = this->random.next() % this->context.granules_per_partition;
      auto txn = this->workload.next_transaction(this->context, partition_id, this->this_cluster_worker_id, granule_id).release();
      txn->context = &this->context;
      auto total_batch_size = this->partitioner->num_coordinator_for_one_replica() * this->context.batch_size;
      if (this->context.stragglers_per_batch) {
        auto v = this->random.uniform_dist(1, total_batch_size);
        if (v <= (uint64_t)this->context.stragglers_per_batch) {
          txn->straggler_wait_time = this->context.stragglers_total_wait_time / this->context.stragglers_per_batch;
        }
      }
      if (this->context.straggler_zipf_factor > 0) {
        int length_type = star::Zipf::globalZipfForStraggler().value(this->random.next_double());
        txn->straggler_wait_time = transaction_lengths[length_type];
        transaction_lengths_count[length_type]++;
      }
      return txn;
    }
  }

  std::chrono::steady_clock::time_point executor_start_time;

  void start() override {
    last_mp_arrival = std::chrono::steady_clock::now();
    LOG(INFO) << "Executor " << (is_replica_worker ? "Replica" : "") << this->id << " starts with thread id" << gettid();

    last_commit = std::chrono::steady_clock::now();
    uint64_t last_seed = 0;
    active_mp_limit = batch_per_worker /3;
    ExecutorStatus status;

    while ((status = static_cast<ExecutorStatus>(this->worker_status.load())) !=
           ExecutorStatus::START) {
      std::this_thread::yield();
    }

    this->n_started_workers.fetch_add(1);
    
    int cnt = 0;
    
    worker_commit = 0;
    int try_times = 0;
    executor_start_time = std::chrono::steady_clock::now();
    bool retry_transaction = false;
    int partition_id;
    bool is_sp = false;
    do {
      drive_event_loop();
      status = static_cast<ExecutorStatus>(this->worker_status.load());
    } while (status != ExecutorStatus::STOP);
    
    LOG(INFO) << "stats memory usage:\n" << memory_usage_from_stats();
    this->n_complete_workers.fetch_add(1);

    // once all workers are stopped, we need to process the replication
    // requests

    while (static_cast<ExecutorStatus>(this->worker_status.load()) !=
           ExecutorStatus::CLEANUP) {
      drive_event_loop(false);
    }

    drive_event_loop(false);
    this->n_complete_workers.fetch_add(1);
    LOG(INFO) << "Executor " << this->id << " exits.";
  }

  void onExit() override {
    // std::string transaction_len_str;
    // for (std::size_t i = 0; i < this->context.straggler_num_txn_len; ++i) {
    //   transaction_len_str += "wait time " + std::to_string(transaction_lengths[i]) + "us, count " + std::to_string(transaction_lengths_count[i]) + "\n";
    // }
    // LOG(INFO) << "Transaction Length Info:\n" << transaction_len_str;
    // std::string message_processing_latency_str = "\nmessage_processing_latency_by_type\n";
    // for (size_t i = 0; i < message_processing_latency.size(); ++i) {
    //   if (message_processing_latency[i].size() > 0) {
    //     message_processing_latency_str += "Message type " + std::to_string(i) + " count " + std::to_string(message_processing_latency[i].size()) + " avg " + std::to_string(message_processing_latency[i].avg()) + " 50th " + std::to_string(message_processing_latency[i].nth(50)) + " 75th " + std::to_string(message_processing_latency[i].nth(75)) + " 95th " + std::to_string(message_processing_latency[i].nth(95)) + "\n";
    //   }
    // }
    // std::string tries_latency_str = "\ntries_latency_str\n";
    // for (size_t i = 0; i < tries_latency.size(); ++i) {
    //   if (tries_latency[i].size() > 0) {
    //     tries_latency_str += "Tries " + std::to_string(i) + " count " + std::to_string(tries_latency[i].size()) + " avg " + std::to_string(tries_latency[i].avg()) + " 50th " + std::to_string(tries_latency[i].nth(50)) + " 75th " + std::to_string(tries_latency[i].nth(75)) + " 95th " + std::to_string(tries_latency[i].nth(95)) +  " 99th " + std::to_string(tries_latency[i].nth(99)) +  " 100th " + std::to_string(tries_latency[i].nth(100)) +"\n";
    //   }
    // }
    // std::string tries_prepare_time_str = "\ntries_prepare_time_str\n";
    // for (size_t i = 0; i < tries_latency.size(); ++i) {
    //   if (tries_prepare_time[i].size() > 0) {
    //     tries_prepare_time_str += "Tries " + std::to_string(i) + " count " + std::to_string(tries_prepare_time[i].size()) + " avg " + std::to_string(tries_prepare_time[i].avg()) + " 50th " + std::to_string(tries_prepare_time[i].nth(50)) + " 75th " + std::to_string(tries_prepare_time[i].nth(75)) + " 95th " + std::to_string(tries_prepare_time[i].nth(95)) + "\n";
    //   }
    // }
    // std::string tries_lock_stall_time_str = "\ntries_lock_stall_time_str\n";
    // for (size_t i = 0; i < tries_latency.size(); ++i) {
    //   if (tries_lock_stall_time[i].size() > 0) {
    //     tries_lock_stall_time_str += "Tries " + std::to_string(i) + " count " + std::to_string(tries_lock_stall_time[i].size()) + " avg " + std::to_string(tries_lock_stall_time[i].avg()) + " 50th " + std::to_string(tries_lock_stall_time[i].nth(50)) + " 75th " + std::to_string(tries_lock_stall_time[i].nth(75)) + " 95th " + std::to_string(tries_lock_stall_time[i].nth(95)) + "\n";
    //   }
    // }
    // std::string tries_execution_done_time_str = "\ntries_execution_done_time_str\n";
    // for (size_t i = 0; i < tries_latency.size(); ++i) {
    //   if (tries_execution_done_latency[i].size() > 0) {
    //     tries_execution_done_time_str += "Tries " + std::to_string(i) + " count " + std::to_string(tries_execution_done_latency[i].size()) + " avg " + std::to_string(tries_execution_done_latency[i].avg()) + " 50th " + std::to_string(tries_execution_done_latency[i].nth(50)) + " 75th " + std::to_string(tries_execution_done_latency[i].nth(75)) + " 95th " + std::to_string(tries_execution_done_latency[i].nth(95)) + "\n";
    //   }
    // }
    // std::string tries_first_lock_response_latency_str = "\ntries_first_lock_response_latency_str\n";
    // for (size_t i = 0; i < tries_latency.size(); ++i) {
    //   if (tries_first_lock_response_latency[i].size() > 0) {
    //     tries_first_lock_response_latency_str += "Tries " + std::to_string(i) + " count " + std::to_string(tries_first_lock_response_latency[i].size()) + " avg " + std::to_string(tries_first_lock_response_latency[i].avg()) + " 50th " + std::to_string(tries_first_lock_response_latency[i].nth(50)) + " 75th " + std::to_string(tries_first_lock_response_latency[i].nth(75)) + " 95th " + std::to_string(tries_first_lock_response_latency[i].nth(95)) + "\n";
    //   }
    // }
    // std::string tries_first_lock_request_arrive_latency_str = "\ntries_first_lock_request_arrive_latency_str\n";
    // for (size_t i = 0; i < tries_latency.size(); ++i) {
    //   if (tries_first_lock_request_arrive_latency[i].size() > 0) {
    //     tries_first_lock_request_arrive_latency_str += "Tries " + std::to_string(i) + " count " + std::to_string(tries_first_lock_request_arrive_latency[i].size()) + " avg " + std::to_string(tries_first_lock_request_arrive_latency[i].avg()) + " 50th " + std::to_string(tries_first_lock_request_arrive_latency[i].nth(50)) + " 75th " + std::to_string(tries_first_lock_request_arrive_latency[i].nth(75)) + " 95th " + std::to_string(tries_first_lock_request_arrive_latency[i].nth(95)) + "\n";
    //   }
    // }
    // std::string tries_first_lock_request_processed_latency_str = "\ntries_first_lock_request_processed_latency_str\n";
    // for (size_t i = 0; i < tries_latency.size(); ++i) {
    //   if (tries_first_lock_request_processed_latency[i].size() > 0) {
    //     tries_first_lock_request_processed_latency_str += "Tries " + std::to_string(i) + " count " + std::to_string(tries_first_lock_request_processed_latency[i].size()) + " avg " + std::to_string(tries_first_lock_request_processed_latency[i].avg()) + " 50th " + std::to_string(tries_first_lock_request_processed_latency[i].nth(50)) + " 75th " + std::to_string(tries_first_lock_request_processed_latency[i].nth(75)) + " 95th " + std::to_string(tries_first_lock_request_processed_latency[i].nth(95)) + "\n";
    //   }
    // }
    // std::string tries_lock_response_latency_str = "\ntries_last_lock_response_latency_str\n";
    // for (size_t i = 0; i < tries_latency.size(); ++i) {
    //   if (tries_lock_response_latency[i].size() > 0) {
    //     tries_lock_response_latency_str += "Tries " + std::to_string(i) + " count " + std::to_string(tries_lock_response_latency[i].size()) + " avg " + std::to_string(tries_lock_response_latency[i].avg()) + " 50th " + std::to_string(tries_lock_response_latency[i].nth(50)) + " 75th " + std::to_string(tries_lock_response_latency[i].nth(75)) + " 95th " + std::to_string(tries_lock_response_latency[i].nth(95)) + "\n";
    //   }
    // }
    // std::string tries_commit_initiated_latency_str = "\ntries_commit_initiated_latency_str\n";
    // for (size_t i = 0; i < tries_latency.size(); ++i) {
    //   if (tries_commit_initiated_latency[i].size() > 0) {
    //     tries_commit_initiated_latency_str += "Tries " + std::to_string(i) + " count " + std::to_string(tries_commit_initiated_latency[i].size()) + " avg " + std::to_string(tries_commit_initiated_latency[i].avg()) + " 50th " + std::to_string(tries_commit_initiated_latency[i].nth(50)) + " 75th " + std::to_string(tries_commit_initiated_latency[i].nth(75)) + " 95th " + std::to_string(tries_commit_initiated_latency[i].nth(95)) + "\n";
    //   }
    // }
    
    LOG(INFO) << (is_replica_worker ? "Replica" : "") << " Worker " << this->id << " commit: "<< this->worker_commit 
              << ". batch concurrency: " << this->round_concurrency.nth(50) 
              << ". effective batch concurrency: " << this->effective_round_concurrency.nth(50) 
              << ". latency: " << this->percentile.nth(50)
              << " us (50%) " << this->percentile.nth(75) << " us (75%) "
              << this->percentile.nth(95) << " us (95%) " << this->percentile.nth(99)
              << " us (99%)  " << this->percentile.avg() << " us (avg). "<< "dist txn latency: " << this->dist_latency.nth(50)
              << " us (50%) " << this->dist_latency.nth(75) << " us (75%) "
              << this->dist_latency.nth(95) << " us (95%) " << this->dist_latency.nth(99)
              << " us (99%) " << " avg " << this->dist_latency.avg() << " us (avg). "
              << " local txn latency: " << this->local_latency.nth(50)
              << " us (50%) " << this->local_latency.nth(75) << " us (75%) "
              << this->local_latency.nth(95) << " us (95%) " << this->local_latency.nth(99)
              << " us (99%). txn try times : " << this->txn_try_times.nth(50)
              << " (50%) " << this->txn_try_times.nth(75) << " (75%) "
              << this->txn_try_times.nth(95) << " (95%) " << this->txn_try_times.nth(99)
              << " (99%). " << " spread_avg_time " << spread_time.avg() << " spread cnt " << spread_cnt.avg() << " replay_query_time " << replay_query_time.avg() << ". "
              << " replica progress query latency " << replica_progress_query_latency.nth(50) << " us(50%), " << replica_progress_query_latency.avg() << " us(avg). "
              << " replication gap " << replication_gap_after_active_replica_execution.avg() 
              << " replication sync comm rounds " << replication_sync_comm_rounds.avg() 
              << " replication time " << replication_time.avg() 
              << " txn tries " << txn_retries.nth(50) << " 50th " << txn_retries.nth(75) << " 75th "  << txn_retries.nth(95) << " 95th " << txn_retries.nth(99) << " 99th " << txn_retries.nth(100) << " 100th "
              << " acquire lock ltc " << acquire_lock_message_latency.nth(50) << " 50th " << acquire_lock_message_latency.nth(75) << " 75th "  << acquire_lock_message_latency.nth(95) << " 95th "
              << " single_partition_mp " << single_worker_mp_txn << " cross_worker_mp " << cross_worker_mp_txn << " cross_node_mp "  << cross_node_mp_txn
              << " replay_loop_time " << replay_loop_time.nth(50) << " " << replay_loop_time.nth(75) << " " << replay_loop_time.nth(95) << " " << replay_loop_time.avg()
              << " replay_concurrency " << replay_mp_concurrency.nth(50) << " " << replay_mp_concurrency.nth(75) << " " << replay_mp_concurrency.nth(95) << " " << replay_mp_concurrency.avg()
              << " round_mp_initiated " << round_mp_initiated.nth(50) << " " << round_mp_initiated.nth(75) << " " << round_mp_initiated.nth(95)
              << " zero_mp_initiated_cost " << zero_mp_initiated_cost.avg()
              << " commit interval " << commit_interval.nth(50) << " " << commit_interval.nth(75) << " " << commit_interval.nth(95) << " " << commit_interval.nth(99) << " " << commit_interval.avg()
              << " mp_arrival_interval " << mp_arrival_interval.nth(50) << " " << mp_arrival_interval.nth(75) << " " << mp_arrival_interval.nth(95) << " " << mp_arrival_interval.avg()
              << " handle request latency " << handle_latency.nth(50) << " " << handle_latency.nth(75) << " " << handle_latency.nth(95) << " " << handle_latency.avg()
              << " handle message count " << handle_msg_cnt.nth(50) << " " << handle_msg_cnt.nth(75) << " " << handle_msg_cnt.nth(95) << " " << handle_msg_cnt.avg()
              << " rtt " << rtt_stat.size() << " " << rtt_stat.nth(50) << " " << rtt_stat.nth(75) << " " << rtt_stat.nth(95) << " " << rtt_stat.avg()
              << " msg_send_latency " << msg_send_latency.nth(50) << " " << msg_send_latency.nth(75) << " " << msg_send_latency.nth(95) << " " << msg_send_latency.avg()
              << " msg_recv_latency " << msg_recv_latency.nth(50) << " " << msg_recv_latency.nth(75) << " " << msg_recv_latency.nth(95) << " " << msg_recv_latency.avg()
              << " msg_proc_latency " << msg_proc_latency.nth(50) << " " << msg_proc_latency.nth(75) << " " << msg_proc_latency.nth(95) << " " << msg_proc_latency.avg()
              //<< message_processing_latency_str
              // << tries_latency_str << tries_prepare_time_str << tries_lock_stall_time_str << tries_execution_done_time_str 
              // << tries_first_lock_request_arrive_latency_str << tries_first_lock_request_processed_latency_str 
              // << tries_first_lock_response_latency_str << tries_lock_response_latency_str << tries_commit_initiated_latency_str
              << " mp_concurrency_limit " << mp_concurrency_limit.avg()
              << " mp_avg_abort_rate " << mp_avg_abort.avg()
              << " cmd queue time " << cmd_queue_time.nth(50)
              << " cmd stall time by lock " << cmd_stall_time.nth(50)
              << " serialize_cmd_time " << serialize_cmd_time.avg()
              << " execution phase time " << execution_phase_time.avg()
              << " execution_after_commit_time " << execution_after_commit_time.avg() 
              << " scheduling_time " << scheduling_cost.avg() << ". \n"
              << " LOCAL count " << this->local_txn_stall_time_pct.size() << " txn stall " << this->local_txn_stall_time_pct.nth(50) << " us, "
              << " local_work " << this->local_txn_local_work_time_pct.avg() << " us, " 
              << " remote_work " << this->local_txn_remote_work_time_pct.avg() << " us, "
              << " commit_work " << this->local_txn_commit_work_time_pct.avg() << " us, "
              << " commit_prepare " << this->local_txn_commit_prepare_time_pct.avg() << " us, "
              << " commit_persistence " << this->local_txn_commit_persistence_time_pct.avg() << " us, "
              << " commit_replication " << this->local_txn_commit_replication_time_pct.avg() << " us, "
              << " commit_write_back " << this->local_txn_commit_write_back_time_pct.avg() << " us, "
              << " commit_release_lock " << this->local_txn_commit_unlock_time_pct.avg() << " us \n"
              << " DIST count " << this->dist_txn_stall_time_pct.size() << " txn stall " << this->dist_txn_stall_time_pct.avg() << " us, "
              << " local_work " << this->dist_txn_local_work_time_pct.avg() << " us "  << this->dist_txn_local_work_time_pct.avg() << " us, "
              << " remote_work " << this->dist_txn_remote_work_time_pct.avg() << " us " << this->dist_txn_remote_work_time_pct.avg() << " us, "
              << " commit_work " << this->dist_txn_commit_work_time_pct.avg() << " us " << this->dist_txn_commit_work_time_pct.avg() << " us, "
              << " commit_prepare " << this->dist_txn_commit_prepare_time_pct.avg() << " us " << this->dist_txn_commit_prepare_time_pct.avg() << " us, "
              << " commit_persistence " << this->dist_txn_commit_persistence_time_pct.avg() << " us, "
              << " commit_replication " << this->dist_txn_commit_replication_time_pct.avg() << " us, "
              << " commit_write_back " << this->dist_txn_commit_write_back_time_pct.avg() << " us " << this->dist_txn_commit_write_back_time_pct.avg() << " us, "
              << " commit_release_lock " << this->dist_txn_commit_unlock_time_pct.avg() << " us\n";
    LOG(INFO) << "STRAGGLER DEBUG INFO:\n" << "count " << straggler_count << "\n" << straggler_mp_debug_string;
    if (this->id == 0) {
      for (auto i = 0u; i < this->message_stats.size(); i++) {
        LOG(INFO) << "message stats, type: " << i
                  << " count: " << this->message_stats[i]
                  << " total size: " << this->message_sizes[i];
      }
      this->percentile.save_cdf(this->context.cdf_path);
    }
  }

protected:

  virtual void flush_messages() override {
    DCHECK(cluster_worker_messages.size() == (size_t)cluster_worker_num);
    //int end_num = is_replica_worker ? cluster_worker_messages.size() : active_replica_worker_num_end;
    // int end_num = cluster_worker_messages.size();
    // for (int i = 0; i < end_num; i++) {
    //   if (cluster_worker_messages[i]->get_message_count() == 0) {
    //     continue;
    //   }

    //   auto message = cluster_worker_messages[i].release();
      
    //   this->out_queue.push(message);
      
    //   cluster_worker_messages[i] = std::make_unique<Message>();
    //   init_message(cluster_worker_messages[i].get(), i);
    // }
    while (!cluster_worker_messages_ready.empty()) {
      int i = cluster_worker_messages_ready.front();
      cluster_worker_messages_ready.pop_front();
      DCHECK(cluster_worker_messages_filled_in[i]);
      if (cluster_worker_messages[i]->get_message_count() == 0) {
        cluster_worker_messages_filled_in[i] = false;
        continue;
      }

      auto message = cluster_worker_messages[i].release();
      
      this->out_queue.push(message);
      
      cluster_worker_messages[i] = std::make_unique<Message>();
      init_message(cluster_worker_messages[i].get(), i);
      cluster_worker_messages_filled_in[i] = false;
    }
  }

  int partition_owner_worker_id_on_a_node(int partition_id) const {
    auto nth_partition_on_master_coord = partition_id / this->partitioner->num_coordinator_for_one_replica();
    if (is_replica_worker == false) {
      DCHECK(this->partitioner->num_coordinator_for_one_replica() == this->hash_partitioner->num_coordinator_for_one_replica());
    }
    auto node_worker_id_this_partition_belongs_to = nth_partition_on_master_coord % this->context.worker_num; // A worker could handle more than 1 partition
    return node_worker_id_this_partition_belongs_to;
  }

  int partition_owner_cluster_coordinator(int partition_id, std::size_t ith_replica) {
    auto ret = this->partitioner->get_ith_replica_coordinator(partition_id, ith_replica);
    if (is_replica_worker == false && ith_replica == 0) {
      DCHECK(ret == this->hash_partitioner->get_ith_replica_coordinator(partition_id, ith_replica));
    }

    return ret;
  }

  int partition_owner_cluster_worker(int partition_id, std::size_t ith_replica) const {
    auto coord_id = this->partitioner->get_ith_replica_coordinator(partition_id, ith_replica);
    if (is_replica_worker == false && ith_replica == 0) {
      DCHECK(coord_id == hash_partitioner->get_ith_replica_coordinator(partition_id, ith_replica));
    }
    auto cluster_worker_id_starts_at_this_node = coord_id * this->context.worker_num;

    auto ret = cluster_worker_id_starts_at_this_node + partition_owner_worker_id_on_a_node(partition_id);
    return ret;
  }

  int cluster_worker_id_to_coordinator_id(int dest_cluster_worker_id) {
    return dest_cluster_worker_id / this->context.worker_num;
  }

  int cluster_worker_id_to_worker_id_on_a_node(int dest_cluster_worker_id) {
    return dest_cluster_worker_id % this->context.worker_num;
  }


  void init_message(Message *message, int dest_cluster_worker_id) {
    //DCHECK(dest_cluster_worker_id >= 0 && dest_cluster_worker_id < (int)this->context.partition_num);
    DCHECK(dest_cluster_worker_id < (int)cluster_worker_num);
    message->set_source_node_id(this->coordinator_id);
    int dest_coord_id = cluster_worker_id_to_coordinator_id(dest_cluster_worker_id);
    message->set_dest_node_id(dest_coord_id);
    int dest_worker_id = cluster_worker_id_to_worker_id_on_a_node(dest_cluster_worker_id);
    message->set_worker_id(dest_worker_id);
    message->set_source_cluster_worker_id(this_cluster_worker_id);
  }
};
} // namespace star
