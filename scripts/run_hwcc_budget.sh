#! /bin/bash

set -uo pipefail
# set -x

typeset SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
typeset current_date_time="`date +%Y%m%d%H%M`"

source $SCRIPT_DIR/common.sh

function print_usage {
        echo "[usage] $0 RESULT_ROOT_DIR"
}

if [ $# != 1 ]; then
        print_usage
        exit -1
fi

typeset RESULT_ROOT_DIR=$1

########### General Configuration BEGIN ###########

# common parameters
typeset HOST_NUM=8
typeset WORKER_NUM=3
typeset OLD_WORKER_NUM=2

typeset DEFAULT_WAL_GROUP_COMMIT_TIME=20000 # 20 ms

typeset DEFAULT_HCC_SIZE_LIMIT=$(( 1024*1024*200 ))     # 200 MB

typeset DATA_MOVEMENT_EXP_HCC_SIZE_LIMIT_1=$(( 1024*1024*200 ))         # 200 MB
typeset DATA_MOVEMENT_EXP_HCC_SIZE_LIMIT_2=$(( 1024*1024*150 ))         # 150 MB
typeset DATA_MOVEMENT_EXP_HCC_SIZE_LIMIT_3=$(( 1024*1024*100 ))         # 100 MB
typeset DATA_MOVEMENT_EXP_HCC_SIZE_LIMIT_4=$(( 1024*1024*50 ))          # 50 MB
typeset DATA_MOVEMENT_EXP_HCC_SIZE_LIMIT_5=$(( 1024*1024*10 ))          # 10 MB

# common parameters for TPCC
typeset TPCC_RUN_TIME=120
typeset TPCC_WARMUP_TIME=60

# common parameters for YCSB
typeset YCSB_RUN_TIME=60
typeset YCSB_WARMUP_TIME=30

typeset READ_INTENSIVE_RW_RATIO=95
typeset WRITE_INTENSIVE_RW_RATIO=50

########### General Configuration END ###########

typeset RESULT_DIR=$RESULT_ROOT_DIR/hwcc_budget
mkdir -p $RESULT_DIR

########### HWcc Budget BEGIN ###########

# TPCC
run_remote_txn_overhead_tpcc $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM 1 0 Clock OnDemand $DATA_MOVEMENT_EXP_HCC_SIZE_LIMIT_1 1 WriteThrough None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $TPCC_RUN_TIME $TPCC_WARMUP_TIME
run_remote_txn_overhead_tpcc $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM 1 0 Clock OnDemand $DATA_MOVEMENT_EXP_HCC_SIZE_LIMIT_2 1 WriteThrough None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $TPCC_RUN_TIME $TPCC_WARMUP_TIME
run_remote_txn_overhead_tpcc $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM 1 0 Clock OnDemand $DATA_MOVEMENT_EXP_HCC_SIZE_LIMIT_3 1 WriteThrough None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $TPCC_RUN_TIME $TPCC_WARMUP_TIME
run_remote_txn_overhead_tpcc $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM 1 0 Clock OnDemand $DATA_MOVEMENT_EXP_HCC_SIZE_LIMIT_4 1 WriteThrough None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $TPCC_RUN_TIME $TPCC_WARMUP_TIME
run_remote_txn_overhead_tpcc $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM 1 0 Clock OnDemand $DATA_MOVEMENT_EXP_HCC_SIZE_LIMIT_5 1 WriteThrough None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $TPCC_RUN_TIME $TPCC_WARMUP_TIME

# YCSB: read-intensive + 0.7 skewness
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.7 1 0 Clock OnDemand $DATA_MOVEMENT_EXP_HCC_SIZE_LIMIT_1 1 WriteThrough None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $YCSB_RUN_TIME $YCSB_WARMUP_TIME
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.7 1 0 Clock OnDemand $DATA_MOVEMENT_EXP_HCC_SIZE_LIMIT_2 1 WriteThrough None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $YCSB_RUN_TIME $YCSB_WARMUP_TIME
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.7 1 0 Clock OnDemand $DATA_MOVEMENT_EXP_HCC_SIZE_LIMIT_3 1 WriteThrough None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $YCSB_RUN_TIME $YCSB_WARMUP_TIME
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.7 1 0 Clock OnDemand $DATA_MOVEMENT_EXP_HCC_SIZE_LIMIT_4 1 WriteThrough None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $YCSB_RUN_TIME $YCSB_WARMUP_TIME
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.7 1 0 Clock OnDemand $DATA_MOVEMENT_EXP_HCC_SIZE_LIMIT_5 1 WriteThrough None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $YCSB_RUN_TIME $YCSB_WARMUP_TIME

########### HWcc Budget END ###########
