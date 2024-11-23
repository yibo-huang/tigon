#include "benchmark/tatp/Database.h"
#include "core/Coordinator.h"
#include "core/Macros.h"
#include "common/WALLogger.h"
#include "common/CXLMemory.h"

DEFINE_int32(cross_ratio, 0, "cross partition transaction ratio");
DEFINE_int32(keys, 10000000, "number of accounts in a partition");

bool do_tid_check = false;

void check_context(star::tatp::Context &context)
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

	star::tatp::Context context;
	SETUP_CONTEXT(context);

	context.crossPartitionProbability = FLAGS_cross_ratio;
	context.numSubScriberPerPartition = FLAGS_keys;

	context.granules_per_partition = FLAGS_granule_count;

	LOG(INFO) << "crossPartitionProbability = " << context.crossPartitionProbability;
        LOG(INFO) << "numSubScriberPerPartition = " << context.numSubScriberPerPartition;
	LOG(INFO) << "granules_per_partition " << context.granules_per_partition;

        check_context(context);

	star::tatp::Database db;
	db.initialize(context);

	do_tid_check = false;
	star::Coordinator c(FLAGS_id, db, context);
	c.connectToPeers();
	c.start();
	return 0;
}
