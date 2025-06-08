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

typeset DEFAULT_WAL_GROUP_COMMIT_TIME=10000 # 10 ms

typeset DEFAULT_HCC_SIZE_LIMIT=$(( 1024*1024*200 ))     # 200 MB

# common parameters for YCSB
typeset YCSB_RUN_TIME=30
typeset YCSB_WARMUP_TIME=10

typeset READ_INTENSIVE_RW_RATIO=95
typeset WRITE_INTENSIVE_RW_RATIO=50

########### General Configuration END ###########

typeset RESULT_DIR=$RESULT_ROOT_DIR/ycsb
mkdir -p $RESULT_DIR

########### YCSB BEGIN ###########

# YCSB: read-only + high-skewness (0.7) + varying percentages of remote operations
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM rmw 100 0.7 1 0 NoMoveOut OnDemand 0 0 NoOP None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $YCSB_RUN_TIME $YCSB_WARMUP_TIME             # TwoPL-CXL-improved
run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM rmw 100 0.7 1 0 NoMoveOut OnDemand 0 0 NoOP None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $YCSB_RUN_TIME $YCSB_WARMUP_TIME           # Sundial-CXL-improved
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw 100 0.7 1 0 Clock OnDemand $DEFAULT_HCC_SIZE_LIMIT 1 WriteThrough NonPart GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $YCSB_RUN_TIME $YCSB_WARMUP_TIME     # Tigon

# YCSB: write-only + high-skewness (0.7) + varying percentages of remote operations
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM rmw 0 0.7 1 0 NoMoveOut OnDemand 0 0 NoOP None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $YCSB_RUN_TIME $YCSB_WARMUP_TIME               # TwoPL-CXL-improved
run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM rmw 0 0.7 1 0 NoMoveOut OnDemand 0 0 NoOP None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $YCSB_RUN_TIME $YCSB_WARMUP_TIME             # Sundial-CXL-improved
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw 0 0.7 1 0 Clock OnDemand $DEFAULT_HCC_SIZE_LIMIT 1 WriteThrough NonPart GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $YCSB_RUN_TIME $YCSB_WARMUP_TIME       # Tigon

# YCSB: read-intensive + high-skewness (0.7) + varying percentages of remote operations
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.7 1 0 NoMoveOut OnDemand 0 0 NoOP None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $YCSB_RUN_TIME $YCSB_WARMUP_TIME           # TwoPL-CXL-improved
run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.7 1 0 NoMoveOut OnDemand 0 0 NoOP None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $YCSB_RUN_TIME $YCSB_WARMUP_TIME         # Sundial-CXL-improved
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $READ_INTENSIVE_RW_RATIO 0.7 1 0 Clock OnDemand $DEFAULT_HCC_SIZE_LIMIT 1 WriteThrough NonPart GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $YCSB_RUN_TIME $YCSB_WARMUP_TIME     # Tigon

# YCSB: write-intensive + high-skewness (0.7) + varying percentages of remote operations
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.7 1 0 NoMoveOut OnDemand 0 0 NoOP None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $YCSB_RUN_TIME $YCSB_WARMUP_TIME           # TwoPL-CXL-improved
run_remote_txn_overhead_ycsb $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.7 1 0 NoMoveOut OnDemand 0 0 NoOP None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $YCSB_RUN_TIME $YCSB_WARMUP_TIME         # Sundial-CXL-improved
run_remote_txn_overhead_ycsb $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM rmw $WRITE_INTENSIVE_RW_RATIO 0.7 1 0 Clock OnDemand $DEFAULT_HCC_SIZE_LIMIT 1 WriteThrough NonPart GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $YCSB_RUN_TIME $YCSB_WARMUP_TIME     # Tigon

########### YCSB END ###########
