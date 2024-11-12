#! /bin/bash

set -uo pipefail
# set -x

typeset SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
typeset current_date_time="`date +%Y%m%d%H%M`"

function print_usage {
        echo "[usage] ./remote_txn_overhead.sh"
}

if [ $# != 0 ]; then
        print_usage
        exit -1
fi

# common parameters
typeset HOST_NUM=8
typeset WORKER_NUM=2

# common parameters for YCSB
typeset KEYS=40960
typeset WRITE_INTENSIVE_RW_RATIO=50
typeset READ_INTENSIVE_RW_RATIO=90

typeset RESULT_DIR=$SCRIPT_DIR/../results/remote_txn_overhead/$current_date_time

function run_remote_txn_overhead_tpcc {
        if [ $# != 10 ]; then
                print_usage
                exit -1
        fi

        typeset RESULT_DIR=$1
        typeset PROTOCOL=$2
        typeset HOST_NUM=$3
        typeset WORKER_NUM=$4
        typeset USE_CXL_TRANS=$5
        typeset MIGRATION_POLICY=$6
        typeset WHEN_TO_MOVE_OUT=$7
        typeset MAX_MIGRATED_ROWS_SIZE=$8
        typeset SCC_MECHANISM=$9
        typeset PRE_MIGRATE=${10}

        typeset TIME_TO_RUN=20
        typeset TIME_TO_WARMUP=5

        typeset RESULT_FILE=$RESULT_DIR/tpcc-$PROTOCOL-$HOST_NUM-$WORKER_NUM-$USE_CXL_TRANS-$MIGRATION_POLICY-$WHEN_TO_MOVE_OUT-$MAX_MIGRATED_ROWS_SIZE-$SCC_MECHANISM-$PRE_MIGRATE.txt

        mkdir -p $RESULT_DIR

        $SCRIPT_DIR/run.sh TPCC $PROTOCOL $HOST_NUM $WORKER_NUM mixed 0 0 $USE_CXL_TRANS $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM None $TIME_TO_RUN $TIME_TO_WARMUP 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh TPCC $PROTOCOL $HOST_NUM $WORKER_NUM mixed 10 15 $USE_CXL_TRANS $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh TPCC $PROTOCOL $HOST_NUM $WORKER_NUM mixed 20 30 $USE_CXL_TRANS $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh TPCC $PROTOCOL $HOST_NUM $WORKER_NUM mixed 30 45 $USE_CXL_TRANS $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh TPCC $PROTOCOL $HOST_NUM $WORKER_NUM mixed 40 60 $USE_CXL_TRANS $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh TPCC $PROTOCOL $HOST_NUM $WORKER_NUM mixed 50 75 $USE_CXL_TRANS $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh TPCC $PROTOCOL $HOST_NUM $WORKER_NUM mixed 60 90 $USE_CXL_TRANS $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP 0 >> $RESULT_FILE 2>&1
}

function run_remote_txn_overhead_ycsb {
        if [ $# != 13 ]; then
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
        typeset MIGRATION_POLICY=$9
        typeset WHEN_TO_MOVE_OUT=${10}
        typeset MAX_MIGRATED_ROWS_SIZE=${11}
        typeset SCC_MECHANISM=${12}
        typeset PRE_MIGRATE=${13}

        typeset TIME_TO_RUN=20
        typeset TIME_TO_WARMUP=5

        typeset RESULT_FILE=$RESULT_DIR/ycsb-$PROTOCOL-$WORKLOAD-$HOST_NUM-$WORKER_NUM-$RW_RATIO-$ZIPF_THETA-$USE_CXL_TRANS-$MIGRATION_POLICY-$WHEN_TO_MOVE_OUT-$MAX_MIGRATED_ROWS_SIZE-$SCC_MECHANISM-$PRE_MIGRATE.txt

        mkdir -p $RESULT_DIR

        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 0 $USE_CXL_TRANS $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM None $TIME_TO_RUN $TIME_TO_WARMUP 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 10 $USE_CXL_TRANS $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 20 $USE_CXL_TRANS $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 30 $USE_CXL_TRANS $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 40 $USE_CXL_TRANS $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 50 $USE_CXL_TRANS $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 60 $USE_CXL_TRANS $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 70 $USE_CXL_TRANS $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 80 $USE_CXL_TRANS $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 90 $USE_CXL_TRANS $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP 0 >> $RESULT_FILE 2>&1
        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $WORKLOAD $KEYS $RW_RATIO $ZIPF_THETA 100 $USE_CXL_TRANS $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS_SIZE $SCC_MECHANISM $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP 0 >> $RESULT_FILE 2>&1
}

mkdir -p $RESULT_DIR

### (custom) YCSB with Uniform Distribution and CXL transport ###
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 WriteThrough NonPart
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 NonTemporal NonPart
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 NoOP NonPart

run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0 1 OnDemandFIFO OnDemand 100000 WriteThrough None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0 1 OnDemandFIFO OnDemand 100000 NonTemporal None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0 1 OnDemandFIFO OnDemand 100000 NoOP None

run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0 1 LRU OnDemand 100000 WriteThrough None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0 1 LRU OnDemand 100000 NonTemporal None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0 1 LRU OnDemand 100000 NoOP None

# run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 NoOP None
# run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 NoOP None

### (custom) YCSB with Uniform Distribution and Network transport ###
# run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0 0 NoMoveOut OnDemand 0 NoOP None
# run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0 0 NoMoveOut OnDemand 0 NoOP None

### (custom) YCSB with High-skewness with CXL transport ###
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 WriteThrough NonPart
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 NonTemporal NonPart
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 NoOP NonPart

run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0.99 1 OnDemandFIFO OnDemand 100000 WriteThrough None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0.99 1 OnDemandFIFO OnDemand 100000 NonTemporal None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0.99 1 OnDemandFIFO OnDemand 100000 NoOP None

run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0.99 1 LRU OnDemand 100000 WriteThrough None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0.99 1 LRU OnDemand 100000 NonTemporal None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0.99 1 LRU OnDemand 100000 NoOP None

# run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 NoOP None
# run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 NoOP None

### (custom) YCSB with High-skewness with Network transport ###
# run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0.99 0 NoMoveOut OnDemand 0 NoOP None
# run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM mixed $WRITE_INTENSIVE_RW_RATIO 0.99 0 NoMoveOut OnDemand 0 NoOP None




### TPCC with CXL transport ###
run_remote_txn_overhead_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 1 NoMoveOut OnDemand 0 WriteThrough NonPart
run_remote_txn_overhead_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 1 NoMoveOut OnDemand 0 NonTemporal NonPart
run_remote_txn_overhead_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 1 NoMoveOut OnDemand 0 NoOP NonPart

run_remote_txn_overhead_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 1 OnDemandFIFO OnDemand 1000000 WriteThrough None
run_remote_txn_overhead_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 1 OnDemandFIFO OnDemand 1000000 NonTemporal None
run_remote_txn_overhead_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 1 OnDemandFIFO OnDemand 1000000 NoOP None

run_remote_txn_overhead_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 1 LRU OnDemand 1000000 WriteThrough None
run_remote_txn_overhead_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 1 LRU OnDemand 1000000 NonTemporal None
run_remote_txn_overhead_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 1 LRU OnDemand 1000000 NoOP None

run_remote_txn_overhead_tpcc $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM 1 NoMoveOut OnDemand 0 WriteThrough NonPart
run_remote_txn_overhead_tpcc $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM 1 NoMoveOut OnDemand 0 NonTemporal NonPart
run_remote_txn_overhead_tpcc $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM 1 NoMoveOut OnDemand 0 NoOP NonPart

run_remote_txn_overhead_tpcc $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM 1 OnDemandFIFO OnDemand 1000000 WriteThrough None
run_remote_txn_overhead_tpcc $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM 1 OnDemandFIFO OnDemand 1000000 NonTemporal None
run_remote_txn_overhead_tpcc $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM 1 OnDemandFIFO OnDemand 1000000 NoOP None

run_remote_txn_overhead_tpcc $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM 1 LRU OnDemand 1000000 WriteThrough None
run_remote_txn_overhead_tpcc $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM 1 LRU OnDemand 1000000 NonTemporal None
run_remote_txn_overhead_tpcc $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM 1 LRU OnDemand 1000000 NoOP None

run_remote_txn_overhead_tpcc $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM 1 NoMoveOut OnDemand 0 NoOP None
run_remote_txn_overhead_tpcc $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM 1 NoMoveOut OnDemand 0 NoOP None

### TPCC with Network transport ###
run_remote_txn_overhead_tpcc $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM 0 NoMoveOut OnDemand 0 NoOP None
run_remote_txn_overhead_tpcc $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM 0 NoMoveOut OnDemand 0 NoOP None




### (write-intensive) YCSB with Uniform Distribution and CXL transport ###
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 WriteThrough NonPart
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 NonTemporal NonPart
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 NoOP NonPart

run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 1 OnDemandFIFO OnDemand 1000000 WriteThrough None
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 1 OnDemandFIFO OnDemand 1000000 NonTemporal None
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 1 OnDemandFIFO OnDemand 1000000 NoOP None

run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 1 LRU OnDemand 1000000 WriteThrough None
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 1 LRU OnDemand 1000000 NonTemporal None
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 1 LRU OnDemand 1000000 NoOP None

run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 WriteThrough NonPart
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 NonTemporal NonPart
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 NoOP NonPart

run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 1 OnDemandFIFO OnDemand 1000000 WriteThrough None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 1 OnDemandFIFO OnDemand 1000000 NonTemporal None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 1 OnDemandFIFO OnDemand 1000000 NoOP None

run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 1 LRU OnDemand 1000000 NonTemporal None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 1 LRU OnDemand 1000000 WriteThrough None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 1 LRU OnDemand 1000000 NoOP None

run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 NoOP None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 NoOP None

### (write-intensive) YCSB with Uniform Distribution and Network transport ###
run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 0 NoMoveOut OnDemand 0 NoOP None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0 0 NoMoveOut OnDemand 0 NoOP None

### (write-intensive) YCSB with High-skewness with CXL transport ###
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 WriteThrough NonPart
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 NonTemporal NonPart
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 NoOP NonPart

run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 1 OnDemandFIFO OnDemand 1000000 WriteThrough None
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 1 OnDemandFIFO OnDemand 1000000 NonTemporal None
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 1 OnDemandFIFO OnDemand 1000000 NoOP None

run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 1 LRU OnDemand 1000000 WriteThrough None
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 1 LRU OnDemand 1000000 NonTemporal None
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 1 LRU OnDemand 1000000 NoOP None

run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 WriteThrough NonPart
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 NonTemporal NonPart
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 NoOP NonPart

run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 1 OnDemandFIFO OnDemand 1000000 WriteThrough None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 1 OnDemandFIFO OnDemand 1000000 NonTemporal None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 1 OnDemandFIFO OnDemand 1000000 NoOP None

run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 1 LRU OnDemand 1000000 WriteThrough None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 1 LRU OnDemand 1000000 NonTemporal None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 1 LRU OnDemand 1000000 NoOP None

run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 NoOP None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 NoOP None

### (write-intensive) YCSB with High-skewness with Network transport ###
run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 0 NoMoveOut OnDemand 0 NoOP None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.99 0 NoMoveOut OnDemand 0 NoOP None




### (read-intensive) YCSB with Uniform Distribution and CXL transport ###
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 WriteThrough NonPart
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 NonTemporal NonPart
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 NoOP NonPart

run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 1 OnDemandFIFO OnDemand 1000000 WriteThrough None
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 1 OnDemandFIFO OnDemand 1000000 NonTemporal None
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 1 OnDemandFIFO OnDemand 1000000 NoOP None

run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 1 LRU OnDemand 1000000 WriteThrough None
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 1 LRU OnDemand 1000000 NonTemporal None
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 1 LRU OnDemand 1000000 NoOP None

run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 WriteThrough NonPart
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 NonTemporal NonPart
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 NoOP NonPart

run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 1 OnDemandFIFO OnDemand 1000000 WriteThrough None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 1 OnDemandFIFO OnDemand 1000000 NonTemporal None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 1 OnDemandFIFO OnDemand 1000000 NoOP None

run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 1 LRU OnDemand 1000000 WriteThrough None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 1 LRU OnDemand 1000000 NonTemporal None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 1 LRU OnDemand 1000000 NoOP None

run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 NoOP None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 1 NoMoveOut OnDemand 0 NoOP None

### (read-intensive) YCSB with Uniform Distribution and Network transport ###
run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 0 NoMoveOut OnDemand 0 NoOP None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0 0 NoMoveOut OnDemand 0 NoOP None

### (read-intensive) YCSB with High-skewness with CXL transport ###
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha rmw $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 WriteThrough NonPart
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha rmw $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 NonTemporal NonPart
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha rmw $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 NoOP NonPart

run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 1 OnDemandFIFO OnDemand 1000000 WriteThrough None
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 1 OnDemandFIFO OnDemand 1000000 NonTemporal None
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 1 OnDemandFIFO OnDemand 1000000 NoOP None

run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 1 LRU OnDemand 1000000 WriteThrough None
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 1 LRU OnDemand 1000000 NonTemporal None
run_remote_txn_overhead_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 1 LRU OnDemand 1000000 NoOP None

run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 WriteThrough NonPart
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 NonTemporal NonPart
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 NoOP NonPart

run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 1 OnDemandFIFO OnDemand 1000000 WriteThrough None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 1 OnDemandFIFO OnDemand 1000000 NonTemporal None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 1 OnDemandFIFO OnDemand 1000000 NoOP None

run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 1 LRU OnDemand 1000000 WriteThrough None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 1 LRU OnDemand 1000000 NonTemporal None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 1 LRU OnDemand 1000000 NoOP None

run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 NoOP None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 1 NoMoveOut OnDemand 0 NoOP None

### (read-intensive) YCSB with High-skewness with Network transport ###
run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 0 NoMoveOut OnDemand 0 NoOP None
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.99 0 NoMoveOut OnDemand 0 NoOP None
