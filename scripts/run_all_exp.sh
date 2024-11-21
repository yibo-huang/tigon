#! /bin/bash

set -uo pipefail
# set -x

typeset SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
typeset current_date_time="`date +%Y%m%d%H%M`"

function print_usage {
        echo "[usage] ./run_all_exp.sh"
}

if [ $# != 0 ]; then
        print_usage
        exit -1
fi

# common parameters
typeset HOST_NUM=8
typeset WORKER_NUM=3
typeset OLD_WORKER_NUM=2
typeset DEFAULT_HCC_SIZE_LIMIT=200000000

# common parameters for TPCC

# common parameters for YCSB
typeset KEYS=40960
typeset WRITE_INTENSIVE_RW_RATIO=50
typeset READ_INTENSIVE_RW_RATIO=90

function run_remote_txn_overhead_tpcc {
        if [ $# != 12 ]; then
                print_usage
                exit -1
        fi

        typeset RESULT_DIR=$1
        typeset PROTOCOL=$2
        typeset HOST_NUM=$3
        typeset WORKER_NUM=$4
        typeset USE_CXL_TRANS=$5
        typeset USE_OUTPUT_THREAD=$6
        typeset MIGRATION_POLICY=$7
        typeset WHEN_TO_MOVE_OUT=$8
        typeset MAX_MIGRATED_ROWS_SIZE=$9
        typeset SCC_MECHANISM=${10}
        typeset PRE_MIGRATE=${11}
        typeset LOGGING_TYPE=${12}

        typeset TIME_TO_RUN=20
        typeset TIME_TO_WARMUP=5

        typeset RESULT_FILE=$RESULT_DIR/tpcc-$PROTOCOL-$HOST_NUM-$WORKER_NUM-$USE_CXL_TRANS-$USE_OUTPUT_THREAD-$MIGRATION_POLICY-$WHEN_TO_MOVE_OUT-$MAX_MIGRATED_ROWS_SIZE-$SCC_MECHANISM-$PRE_MIGRATE-$LOGGING_TYPE.txt

        mkdir -p $RESULT_DIR

        $SCRIPT_DIR/run.sh TPCC $PROTOCOL $HOST_NUM $WORKER_NUM mixed 0 0 $USE_CXL_TRANS $USE_OUTPUT_THREAD $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM None $TIME_TO_RUN $TIME_TO_WARMUP $LOGGING_TYPE 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh TPCC $PROTOCOL $HOST_NUM $WORKER_NUM mixed 10 15 $USE_CXL_TRANS $USE_OUTPUT_THREAD $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP $LOGGING_TYPE 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh TPCC $PROTOCOL $HOST_NUM $WORKER_NUM mixed 20 30 $USE_CXL_TRANS $USE_OUTPUT_THREAD $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP $LOGGING_TYPE 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh TPCC $PROTOCOL $HOST_NUM $WORKER_NUM mixed 30 45 $USE_CXL_TRANS $USE_OUTPUT_THREAD $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP $LOGGING_TYPE 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh TPCC $PROTOCOL $HOST_NUM $WORKER_NUM mixed 40 60 $USE_CXL_TRANS $USE_OUTPUT_THREAD $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP $LOGGING_TYPE 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh TPCC $PROTOCOL $HOST_NUM $WORKER_NUM mixed 50 75 $USE_CXL_TRANS $USE_OUTPUT_THREAD $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP $LOGGING_TYPE 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh TPCC $PROTOCOL $HOST_NUM $WORKER_NUM mixed 60 90 $USE_CXL_TRANS $USE_OUTPUT_THREAD $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP $LOGGING_TYPE 0 >> $RESULT_FILE 2>&1
}

function run_remote_txn_overhead_ycsb {
        if [ $# != 15 ]; then
                print_usage
                exit -1
        fi

        typeset RESULT_DIR=$1
        typeset PROTOCOL=$2
        typeset HOST_NUM=$3
        typeset WORKER_NUM=$4
        typeset WORKLOAD=$5
        typeset RW_RATIO=$6
        typeset ZIPF_THETA=$7
        typeset USE_CXL_TRANS=$8
        typeset USE_OUTPUT_THREAD=$9
        typeset MIGRATION_POLICY=${10}
        typeset WHEN_TO_MOVE_OUT=${11}
        typeset MAX_MIGRATED_ROWS_SIZE=${12}
        typeset SCC_MECHANISM=${13}
        typeset PRE_MIGRATE=${14}
        typeset LOGGING_TYPE=${15}

        typeset TIME_TO_RUN=15
        typeset TIME_TO_WARMUP=5

        typeset RESULT_FILE=$RESULT_DIR/ycsb-$PROTOCOL-$WORKLOAD-$HOST_NUM-$WORKER_NUM-$RW_RATIO-$ZIPF_THETA-$USE_CXL_TRANS-$USE_OUTPUT_THREAD-$MIGRATION_POLICY-$WHEN_TO_MOVE_OUT-$MAX_MIGRATED_ROWS_SIZE-$SCC_MECHANISM-$PRE_MIGRATE-$LOGGING_TYPE.txt

        mkdir -p $RESULT_DIR

        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 0 $USE_CXL_TRANS $USE_OUTPUT_THREAD $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM None $TIME_TO_RUN $TIME_TO_WARMUP $LOGGING_TYPE 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 10 $USE_CXL_TRANS $USE_OUTPUT_THREAD $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP $LOGGING_TYPE 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 20 $USE_CXL_TRANS $USE_OUTPUT_THREAD $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP $LOGGING_TYPE 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 30 $USE_CXL_TRANS $USE_OUTPUT_THREAD $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP $LOGGING_TYPE 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 40 $USE_CXL_TRANS $USE_OUTPUT_THREAD $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP $LOGGING_TYPE 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 50 $USE_CXL_TRANS $USE_OUTPUT_THREAD $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP $LOGGING_TYPE 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 60 $USE_CXL_TRANS $USE_OUTPUT_THREAD $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP $LOGGING_TYPE 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 70 $USE_CXL_TRANS $USE_OUTPUT_THREAD $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP $LOGGING_TYPE 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 80 $USE_CXL_TRANS $USE_OUTPUT_THREAD $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP $LOGGING_TYPE 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 90 $USE_CXL_TRANS $USE_OUTPUT_THREAD $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP $LOGGING_TYPE 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 100 $USE_CXL_TRANS $USE_OUTPUT_THREAD $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP $LOGGING_TYPE 0 >> $RESULT_FILE 2>&1
}


########### Microbenchmark BEGIN ###########
typeset RESULT_DIR=$SCRIPT_DIR/../results/$current_date_time/microbenchmark
mkdir -p $RESULT_DIR

# YCSB: read-only + uniform distribution + varying percentages of remote operations
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw 100 0 1 0 LRU OnDemand $DEFAULT_HCC_SIZE_LIMIT WriteThrough NonPart GROUP_WAL

run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM rmw 100 0 1 0 NoMoveOut OnDemand 0 NoOP None GROUP_WAL             # TwoPL-CXL-improved
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $OLD_WORKER_NUM rmw 100 0 1 1 NoMoveOut OnDemand 0 NoOP None GROUP_WAL         # TwoPL-CXL
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $OLD_WORKER_NUM rmw 100 0 0 1 NoMoveOut OnDemand 0 NoOP None GROUP_WAL         # TwoPL-NET

run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM rmw 100 0 1 0 NoMoveOut OnDemand 0 NoOP None GROUP_WAL           # Sundial-CXL-improved
run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $OLD_WORKER_NUM rmw 100 0 1 1 NoMoveOut OnDemand 0 NoOP None GROUP_WAL       # Sundial-CXL
run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $OLD_WORKER_NUM rmw 100 0 0 1 NoMoveOut OnDemand 0 NoOP None GROUP_WAL       # Sundial-NET

# YCSB: write-only + uniform distribution + varying percentages of remote operations
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw 0 0 1 0 LRU OnDemand $DEFAULT_HCC_SIZE_LIMIT WriteThrough NonPart GROUP_WAL

run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM rmw 0 0 1 0 NoMoveOut OnDemand 0 NoOP None GROUP_WAL               # TwoPL-CXL-improved
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $OLD_WORKER_NUM rmw 0 0 1 1 NoMoveOut OnDemand 0 NoOP None GROUP_WAL           # TwoPL-CXL
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $OLD_WORKER_NUM rmw 0 0 0 1 NoMoveOut OnDemand 0 NoOP None GROUP_WAL           # TwoPL-NET

run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM rmw 0 0 1 0 NoMoveOut OnDemand 0 NoOP None GROUP_WAL             # Sundial-CXL-improved
run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $OLD_WORKER_NUM rmw 0 0 1 1 NoMoveOut OnDemand 0 NoOP None GROUP_WAL         # Sundial-CXL
run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $OLD_WORKER_NUM rmw 0 0 0 1 NoMoveOut OnDemand 0 NoOP None GROUP_WAL         # Sundial-NET

# YCSB: scan-only + uniform distribution + varying percentages of remote operations
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM scan 0 0 1 0 LRU OnDemand $DEFAULT_HCC_SIZE_LIMIT WriteThrough NonPart GROUP_WAL

# YCSB: insert-only + pre-determined pattern + varying percentages of remote operations

########### Microbenchmark END ###########





########### End-to-End Performance BEGIN ###########
typeset RESULT_DIR=$SCRIPT_DIR/../results/$current_date_time/macrobenchmark
mkdir -p $RESULT_DIR

# TPCC
run_remote_txn_overhead_tpcc $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM 1 0 LRU OnDemand $DEFAULT_HCC_SIZE_LIMIT WriteThrough NonPart GROUP_WAL
run_remote_txn_overhead_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 1 0 LRU OnDemand $DEFAULT_HCC_SIZE_LIMIT WriteThrough NonPart GROUP_WAL

run_remote_txn_overhead_tpcc $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM 1 0 NoMoveOut OnDemand 0 NoOP None GROUP_WAL       # TwoPL-CXL-improved
run_remote_txn_overhead_tpcc $RESULT_DIR TwoPL $HOST_NUM $OLD_WORKER_NUM 1 1 NoMoveOut OnDemand 0 NoOP None GROUP_WAL   # TwoPL-CXL
run_remote_txn_overhead_tpcc $RESULT_DIR TwoPL $HOST_NUM $OLD_WORKER_NUM 0 1 NoMoveOut OnDemand 0 NoOP None GROUP_WAL   # TwoPL-NET

run_remote_txn_overhead_tpcc $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM 1 0 NoMoveOut OnDemand 0 NoOP None GROUP_WAL     # Sundial-CXL-improved
run_remote_txn_overhead_tpcc $RESULT_DIR Sundial $HOST_NUM $OLD_WORKER_NUM 1 1 NoMoveOut OnDemand 0 NoOP None GROUP_WAL # Sundial-CXL
run_remote_txn_overhead_tpcc $RESULT_DIR Sundial $HOST_NUM $OLD_WORKER_NUM 0 1 NoMoveOut OnDemand 0 NoOP None GROUP_WAL # Sundial-NET

# TATP

# SmallBank

########### End-to-End Performance END ###########





########### Data Movement BEGIN ###########

########### Data Movement END ###########





########### Software Cache-Coherence BEGIN ###########

########### Software Cache-Coherence END ###########





########## Custom YCSB BEGIN ##########

########## Custom YCSB END ##########
