//
// Created by Yi Lu on 3/18/19.
//

#pragma once

#include "glog/logging.h"
#include <boost/algorithm/string/split.hpp>

DEFINE_string(servers, "127.0.0.1:10010",
              "semicolon-separated list of servers");
DEFINE_int32(id, 0, "coordinator id");
DEFINE_int32(threads, 1, "the number of threads");
DEFINE_int32(io, 1, "the number of i/o threads");
DEFINE_int32(partition_num, 1, "the number of partitions");
DEFINE_string(partitioner, "hash", "database partitioner (hash, hash2, pb)");
DEFINE_bool(sleep_on_retry, true, "sleep when retry aborted transactions");
DEFINE_int32(batch_size, 100, "star or calvin batch size");
DEFINE_int32(group_time, 10, "group commit frequency");
DEFINE_int32(batch_flush, 50, "batch flush");
DEFINE_int32(sleep_time, 100, "retry sleep time");
DEFINE_string(protocol, "Scar", "transaction protocol");
DEFINE_string(replica_group, "1,3", "calvin replica group");
DEFINE_string(lock_manager, "1,1", "calvin lock manager");
DEFINE_bool(read_on_replica, false, "read from replicas");
DEFINE_bool(local_validation, false, "local validation");
DEFINE_bool(rts_sync, false, "rts sync");
DEFINE_bool(star_sync, false, "synchronous write in the single-master phase");
DEFINE_bool(star_dynamic_batch_size, true, "dynamic batch size");
DEFINE_bool(plv, true, "parallel locking and validation");
DEFINE_bool(calvin_same_batch, false, "always run the same batch of txns.");
DEFINE_bool(kiva_read_only, true, "kiva read only optimization");
DEFINE_bool(kiva_reordering, true, "kiva reordering optimization");
DEFINE_bool(kiva_si, false, "kiva snapshot isolation");
DEFINE_int32(delay, 0, "delay time in us.");
DEFINE_string(cdf_path, "", "path to cdf");
DEFINE_string(log_path, "", "path to disk logging.");
DEFINE_bool(tcp_no_delay, true, "TCP Nagle algorithm, true: disable nagle");
DEFINE_bool(tcp_quick_ack, false, "TCP quick ack mode, true: enable quick ack");
DEFINE_bool(enable_hstore_master, true, "enable hstore master for lock scheduling");
DEFINE_bool(cpu_affinity, false, "pinning each thread to a separate core");
DEFINE_bool(hstore_command_logging, true, "configure command logging mode for hstore");
DEFINE_int32(cross_txn_workers, 0, "number of workers generating cross-partition transactions");
DEFINE_int32(cpu_core_id, 0, "cpu core id");
DEFINE_int32(persist_latency, 110, "emulated persist latency");
DEFINE_int32(wal_group_commit_time, 10, "wal group commit time in us");
DEFINE_int32(wal_group_commit_size, 7, "wal group commit batch size");
DEFINE_bool(aria_read_only, true, "aria read only optimization");
DEFINE_bool(aria_reordering, true, "aria reordering optimization");
DEFINE_bool(aria_si, false, "aria snapshot isolation");
DEFINE_int32(stragglers_per_batch, 0, "# stragglers in a batch");
DEFINE_int32(stragglers_num_txn_len, 10, "# straggler transaction length types"); 
DEFINE_int32(stragglers_partition, -1, "straggler partition");
DEFINE_bool(lotus_async_repl, false, "Lotus async replication");
// lotus_checkpoint = 0, COW table off, checkpoint trigger off, logging off
// lotus_checkpoint = 1, COW table off, checkpoint trigger off, logging on
// lotus_checkpoint = 2, COW table on, checkpoint trigger off, logging on
// lotus_checkpoint = 3, COW table on, checkpoint trigger on, logging off
// lotus_checkpoint = 4, COW table on, checkpoint trigger on, logging on, log files and checkpoints are stored on the same disk
enum LotusCheckpointScheme {
  COW_OFF_CHECKPOINT_OFF_LOGGING_OFF = 0,
  COW_OFF_CHECKPOINT_OFF_LOGGING_ON = 1,
  COW_ON_CHECKPOINT_OFF_LOGGING_ON = 2,
  COW_ON_CHECKPOINT_ON_LOGGING_OFF = 3,
  COW_ON_CHECKPOINT_ON_LOGGING_ON = 4
};
DEFINE_int32(lotus_checkpoint, 0, "Lotus COW checkpoint scheme");
DEFINE_string(lotus_checkpoint_location, "", "Path to store checkpoint files");
DEFINE_double(stragglers_zipf_factor, 0, "straggler zipfian factor");
DEFINE_int32(sender_group_nop_count, 40000, "# nop insts to executes during TCP sender message grouping");
DEFINE_int32(granule_count, 1, "# granules in a partition");
DEFINE_bool(hstore_active_active, false, "H-Store style active-active replication");

#define SETUP_CONTEXT(context)                                                 \
  boost::algorithm::split(context.peers, FLAGS_servers,                        \
                          boost::is_any_of(";"));                              \
  context.coordinator_num = context.peers.size();                              \
  context.coordinator_id = FLAGS_id;                                           \
  context.worker_num = FLAGS_threads;                                          \
  context.io_thread_num = FLAGS_io;                                            \
  context.partition_num = FLAGS_partition_num;                                 \
  context.partitioner = FLAGS_partitioner;                                     \
  context.sleep_on_retry = FLAGS_sleep_on_retry;                               \
  context.batch_size = FLAGS_batch_size;                                       \
  context.group_time = FLAGS_group_time;                                       \
  context.batch_flush = FLAGS_batch_flush;                                     \
  context.sleep_time = FLAGS_sleep_time;                                       \
  context.protocol = FLAGS_protocol;                                           \
  context.replica_group = FLAGS_replica_group;                                 \
  context.lock_manager = FLAGS_lock_manager;                                   \
  context.read_on_replica = FLAGS_read_on_replica;                             \
  context.local_validation = FLAGS_local_validation;                           \
  context.rts_sync = FLAGS_rts_sync;                                           \
  context.star_sync_in_single_master_phase = FLAGS_star_sync;                  \
  context.star_dynamic_batch_size = FLAGS_star_dynamic_batch_size;             \
  context.parallel_locking_and_validation = FLAGS_plv;                         \
  context.calvin_same_batch = FLAGS_calvin_same_batch;                         \
  context.kiva_read_only_optmization = FLAGS_kiva_read_only;                   \
  context.kiva_reordering_optmization = FLAGS_kiva_reordering;                 \
  context.kiva_snapshot_isolation = FLAGS_kiva_si;                             \
  context.delay_time = FLAGS_delay;                                            \
  context.log_path = FLAGS_log_path;                                           \
  context.cdf_path = FLAGS_cdf_path;                                           \
  context.tcp_no_delay = FLAGS_tcp_no_delay;                                   \
  context.tcp_quick_ack = FLAGS_tcp_quick_ack;                                 \
  context.cpu_affinity = FLAGS_cpu_affinity;                                   \
  context.enable_hstore_master = FLAGS_enable_hstore_master;                   \
  context.cpu_core_id = FLAGS_cpu_core_id;                                     \
  context.cross_txn_workers = FLAGS_cross_txn_workers;                         \
  context.emulated_persist_latency = FLAGS_persist_latency;                    \
  context.wal_group_commit_time = FLAGS_wal_group_commit_time;                 \
  context.hstore_command_logging = FLAGS_hstore_command_logging;               \
  context.group_commit_batch_size = FLAGS_wal_group_commit_size;               \
  context.aria_read_only_optmization = FLAGS_aria_read_only;                   \
  context.aria_reordering_optmization = FLAGS_aria_reordering;                 \
  context.aria_snapshot_isolation = FLAGS_aria_si;                             \
  context.stragglers_per_batch = FLAGS_stragglers_per_batch;                   \
  context.stragglers_partition = FLAGS_stragglers_partition;                   \
  context.sender_group_nop_count = FLAGS_sender_group_nop_count;               \
  context.straggler_zipf_factor = FLAGS_stragglers_zipf_factor;                \
  context.straggler_num_txn_len = FLAGS_stragglers_num_txn_len;                \
  context.granules_per_partition = FLAGS_granule_count;                        \
  context.lotus_async_repl = FLAGS_lotus_async_repl;                           \
  context.lotus_checkpoint = FLAGS_lotus_checkpoint;                           \
  context.lotus_checkpoint_location = FLAGS_lotus_checkpoint_location;         \
  context.hstore_active_active = FLAGS_hstore_active_active;                   \
  context.set_star_partitioner();
