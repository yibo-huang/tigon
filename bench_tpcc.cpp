#include "benchmark/tpcc/Database.h"
#include "core/Coordinator.h"
#include "core/Macros.h"
#include "common/CXLMemory.h"


DEFINE_bool(operation_replication, false, "use operation replication");
DEFINE_string(query, "neworder", "tpcc query, mixed, neworder, payment, test");
DEFINE_int32(neworder_dist, 10, "new order distributed.");
DEFINE_int32(payment_dist, 15, "payment distributed.");

// ./main --logtostderr=1 --id=1 --servers="127.0.0.1:10010;127.0.0.1:10011"
// cmake -DCMAKE_BUILD_TYPE=Release
bool do_tid_check = false;

void check_context(star::tpcc::Context &context)
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

	star::tpcc::Context context;
	SETUP_CONTEXT(context);

	context.operation_replication = FLAGS_operation_replication;

	context.granules_per_partition = FLAGS_granule_count;

	if (FLAGS_query == "mixed") {
		context.workloadType = star::tpcc::TPCCWorkloadType::MIXED;
        } else if (FLAGS_query == "first_two") {
		context.workloadType = star::tpcc::TPCCWorkloadType::FIRST_TWO;
	} else if (FLAGS_query == "neworder") {
		context.workloadType = star::tpcc::TPCCWorkloadType::NEW_ORDER_ONLY;
	} else if (FLAGS_query == "payment") {
		context.workloadType = star::tpcc::TPCCWorkloadType::PAYMENT_ONLY;
        } else if (FLAGS_query == "test") {
		context.workloadType = star::tpcc::TPCCWorkloadType::TEST;
	} else {
		CHECK(false);
	}

	context.newOrderCrossPartitionProbability = FLAGS_neworder_dist;
	context.paymentCrossPartitionProbability = FLAGS_payment_dist;

        check_context(context);

	star::tpcc::Database db;
	db.initialize(context);

        db.check_consistency(context);

	do_tid_check = false;
	star::Coordinator c(FLAGS_id, db, context);
	c.connectToPeers();
	c.start();

        db.check_consistency(context);

	return 0;
}
