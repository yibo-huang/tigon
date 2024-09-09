#! /bin/bash

set -uo pipefail
# set -x

typeset SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
typeset current_date_time="`date +%Y%m%d%H%M`"

function print_usage {
        echo "[usage] ./migration_policy_stats.sh"
}

if [ $# != 0 ]; then
        print_usage
        exit -1
fi

# common parameters
typeset HOST_NUM=8
typeset WORKER_NUM=2
typeset USE_CXL_TRANS=1
typeset CXL_TRANS_ENTRY_NUM=4096
typeset RESULT_DIR=$SCRIPT_DIR/../results/migration_policy_stats/$current_date_time

function run_migration_policy_stats_tpcc {
        typeset RESULT_DIR=$1
        typeset PROTOCOL=$2
        typeset HOST_NUM=$3
        typeset WORKER_NUM=$4
        typeset REMOTE_NEWORDER_PERC=$5
        typeset REMOTE_PAYMENT_PERC=$6
        typeset USE_CXL_TRANS=$7
        typeset CXL_TRANS_ENTRY_NUM=$8
        typeset MIGRATION_POLICY=$9
        typeset WHEN_TO_MOVE_OUT=${10}
        typeset MAX_MIGRATED_ROWS=${11}

        typeset RESULT_FILE=$RESULT_DIR/tpcc-$PROTOCOL-$MIGRATION_POLICY-$WHEN_TO_MOVE_OUT-$MAX_MIGRATED_ROWS.txt

        mkdir -p $RESULT_DIR

        $SCRIPT_DIR/run.sh TPCC $PROTOCOL $HOST_NUM $WORKER_NUM $REMOTE_NEWORDER_PERC $REMOTE_PAYMENT_PERC $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS 90 60 0 >> $RESULT_FILE 2>&1
}

function run_migration_policy_stats_ycsb {
        typeset RESULT_DIR=$1
        typeset PROTOCOL=$2
        typeset HOST_NUM=$3
        typeset WORKER_NUM=$4
        typeset KEYS=$5
        typeset RW_RATIO=$6
        typeset ZIPF_THETA=$7
        typeset CROSS_RATIO=$8
        typeset USE_CXL_TRANS=$9
        typeset CXL_TRANS_ENTRY_NUM=${10}
        typeset MIGRATION_POLICY=${11}
        typeset WHEN_TO_MOVE_OUT=${12}
        typeset MAX_MIGRATED_ROWS=${13}

        typeset RESULT_FILE=$RESULT_DIR/ycsb-$PROTOCOL-$MIGRATION_POLICY-$WHEN_TO_MOVE_OUT-$MAX_MIGRATED_ROWS.txt

        mkdir -p $RESULT_DIR

        $SCRIPT_DIR/run.sh YCSB $PROTOCOL $HOST_NUM $WORKER_NUM $KEYS $RW_RATIO $ZIPF_THETA $CROSS_RATIO $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $MAX_MIGRATED_ROWS 60 30 0 >> $RESULT_FILE 2>&1
}

mkdir -p $RESULT_DIR

# TPCC
run_migration_policy_stats_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 20 30 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM NoMoveOut OnDemand 100

run_migration_policy_stats_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 20 30 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM OnDemandFIFO OnDemand 100
run_migration_policy_stats_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 20 30 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM OnDemandFIFO OnDemand 1000
run_migration_policy_stats_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 20 30 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM OnDemandFIFO OnDemand 10000
run_migration_policy_stats_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 20 30 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM OnDemandFIFO OnDemand 50000
run_migration_policy_stats_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 20 30 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM OnDemandFIFO OnDemand 100000

run_migration_policy_stats_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 20 30 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM Eagerly OnDemand 100
run_migration_policy_stats_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 20 30 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM Eagerly OnDemand 1000
run_migration_policy_stats_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 20 30 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM Eagerly OnDemand 10000
run_migration_policy_stats_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 20 30 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM Eagerly OnDemand 50000
run_migration_policy_stats_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 20 30 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM Eagerly OnDemand 100000

run_migration_policy_stats_tpcc $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 20 30 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM Eagerly Reactive 1

# YCSB (Uniform Distribution)
run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM NoMoveOut OnDemand 100

run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM OnDemandFIFO OnDemand 100
run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM OnDemandFIFO OnDemand 1000
run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM OnDemandFIFO OnDemand 10000
run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM OnDemandFIFO OnDemand 50000
run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM OnDemandFIFO OnDemand 100000

run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM Eagerly OnDemand 100
run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM Eagerly OnDemand 1000
run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM Eagerly OnDemand 10000
run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM Eagerly OnDemand 50000
run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM Eagerly OnDemand 100000

run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM Eagerly Reactive 1

# YCSB (High-skewness)
run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0.99 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM NoMoveOut OnDemand 100

run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0.99 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM OnDemandFIFO OnDemand 100
run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0.99 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM OnDemandFIFO OnDemand 1000
run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0.99 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM OnDemandFIFO OnDemand 10000
run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0.99 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM OnDemandFIFO OnDemand 50000
run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0.99 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM OnDemandFIFO OnDemand 100000

run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0.99 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM Eagerly OnDemand 100
run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0.99 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM Eagerly OnDemand 1000
run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0.99 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM Eagerly OnDemand 10000
run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0.99 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM Eagerly OnDemand 50000
run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0.99 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM Eagerly OnDemand 100000

run_migration_policy_stats_ycsb $RESULT_DIR SundialPasha $HOST_NUM $WORKER_NUM 40960 50 0.99 20 $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM Eagerly Reactive 1
