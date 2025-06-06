#include "benchmark/ycsb/Database.h"
#include "core/Coordinator.h"
#include "core/Macros.h"
#include "common/WALLogger.h"
#include "common/CXLMemory.h"

DEFINE_string(query, "rmw", "ycsb query, mixed, rmw, scan");
DEFINE_bool(lotus_sp_parallel_exec_commit, false, "parallel execution and commit for Lotus");
DEFINE_int32(read_write_ratio, 80, "read write ratio");
DEFINE_int32(read_only_ratio, 0, "read only transaction ratio");
DEFINE_int32(cross_ratio, 0, "cross partition transaction ratio");
DEFINE_int32(keys, 200000, "keys in a partition.");
DEFINE_double(zipf, 0, "skew factor");
DEFINE_int32(cross_part_num, 2, "Cross-partition partion #");

DEFINE_int32(nop_prob, 0, "prob of transactions having nop, out of 10000");
DEFINE_int64(n_nop, 0, "total number of nop");

// ./main --logtostderr=1 --id=1 --servers="127.0.0.1:10010;127.0.0.1:10011"
// cmake -DCMAKE_BUILD_TYPE=Release

bool do_tid_check = false;

void check_context(star::ycsb::Context &context)
{
        // CXL transport supports single consumer only
        if (context.use_cxl_transport == true)
                DCHECK(context.io_thread_num == 1);
}

int main(int argc, char *argv[])
{
	google::InitGoogleLogging(argv[0]);
	google::InstallFailureSignalHandler();
	google::ParseCommandLineFlags(&argc, &argv, true);

	star::ycsb::Context context;
	SETUP_CONTEXT(context);

        if (FLAGS_query == "mixed") {
		context.workloadType = star::ycsb::YCSBWorkloadType::MIXED;
	} else if (FLAGS_query == "rmw") {
		context.workloadType = star::ycsb::YCSBWorkloadType::RMW;
        } else if (FLAGS_query == "scan") {
		context.workloadType = star::ycsb::YCSBWorkloadType::SCAN;
        } else if (FLAGS_query == "insert") {
		context.workloadType = star::ycsb::YCSBWorkloadType::INSERT;
        } else if (FLAGS_query == "delete") {
		context.workloadType = star::ycsb::YCSBWorkloadType::DELETE;
	} else {
		CHECK(false);
	}

	context.readWriteRatio = FLAGS_read_write_ratio;
	context.readOnlyTransaction = FLAGS_read_only_ratio;
	context.crossPartitionProbability = FLAGS_cross_ratio;
	context.keysPerPartition = FLAGS_keys;
	context.lotus_sp_parallel_exec_commit = FLAGS_lotus_sp_parallel_exec_commit;
	context.crossPartitionPartNum = FLAGS_cross_part_num;
	context.nop_prob = FLAGS_nop_prob;
	context.n_nop = FLAGS_n_nop;

	context.granules_per_partition = FLAGS_granule_count;
	context.keysPerGranule = context.keysPerPartition / context.granules_per_partition;

	LOG(INFO) << "RW_RATIO = " << context.readWriteRatio << ", ZIPF_THETA = " << FLAGS_zipf << ", CROSS_RATIO = " << context.crossPartitionProbability;
	LOG(INFO) << "checkpoint " << context.lotus_checkpoint << " to " << context.lotus_checkpoint_location;
	LOG(INFO) << "cross_part_num " << FLAGS_cross_part_num;
	LOG(INFO) << "lotus_sp_parallel_exec_commit " << FLAGS_lotus_sp_parallel_exec_commit;
	LOG(INFO) << "granules_per_partition " << context.granules_per_partition;
	LOG(INFO) << "keysPerGranule " << context.keysPerGranule;

	star::ycsb::Context::unit_testing(&context);
	if (FLAGS_zipf > 0) {
		context.isUniform = false;
		star::Zipf::globalZipf().init(context.keysPerPartition, FLAGS_zipf);
	}

	if (FLAGS_stragglers_zipf_factor > 0) {
		star::Zipf::globalZipfForStraggler().init(context.straggler_num_txn_len, FLAGS_stragglers_zipf_factor);
	}

        check_context(context);

	star::ycsb::Database db;
	db.initialize(context);

	do_tid_check = false;
	star::Coordinator c(FLAGS_id, db, context);
	c.connectToPeers();
	c.start();
	return 0;
}
