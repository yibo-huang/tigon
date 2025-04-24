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

# common parameters for TPCC
typeset TPCC_RUN_TIME=30
typeset TPCC_WARMUP_TIME=10

########### General Configuration END ###########

typeset RESULT_DIR=$RESULT_ROOT_DIR/tpcc
mkdir -p $RESULT_DIR

########### Baselines BEGIN ###########

run_remote_txn_overhead_tpcc $RESULT_DIR TwoPL $HOST_NUM $WORKER_NUM 1 0 NoMoveOut OnDemand 0 0 NoOP None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $TPCC_RUN_TIME $TPCC_WARMUP_TIME              # TwoPL-CXL-improved
run_remote_txn_overhead_tpcc $RESULT_DIR Sundial $HOST_NUM $WORKER_NUM 1 0 NoMoveOut OnDemand 0 0 NoOP None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $TPCC_RUN_TIME $TPCC_WARMUP_TIME            # Sundial-CXL-improved
run_remote_txn_overhead_tpcc $RESULT_DIR TwoPL $HOST_NUM $OLD_WORKER_NUM 1 1 NoMoveOut OnDemand 0 0 NoOP None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $TPCC_RUN_TIME $TPCC_WARMUP_TIME          # TwoPL-CXL
run_remote_txn_overhead_tpcc $RESULT_DIR Sundial $HOST_NUM $OLD_WORKER_NUM 1 1 NoMoveOut OnDemand 0 0 NoOP None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $TPCC_RUN_TIME $TPCC_WARMUP_TIME        # Sundial-CXL
run_remote_txn_overhead_tpcc $RESULT_DIR TwoPL $HOST_NUM $OLD_WORKER_NUM 0 1 NoMoveOut OnDemand 0 0 NoOP None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $TPCC_RUN_TIME $TPCC_WARMUP_TIME          # TwoPL-NET
run_remote_txn_overhead_tpcc $RESULT_DIR Sundial $HOST_NUM $OLD_WORKER_NUM 0 1 NoMoveOut OnDemand 0 0 NoOP None GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $TPCC_RUN_TIME $TPCC_WARMUP_TIME        # Sundial-NET

########### Baselines END ###########


########### Tigon End-to-End Performance BEGIN ###########

run_remote_txn_overhead_tpcc $RESULT_DIR TwoPLPasha $HOST_NUM $WORKER_NUM 1 0 Clock OnDemand $DEFAULT_HCC_SIZE_LIMIT 1 WriteThrough NonPart GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $TPCC_RUN_TIME $TPCC_WARMUP_TIME               # Tigon-TwoPL
# run_remote_txn_overhead_tpcc $RESULT_DIR TwoPLPashaPhantom $HOST_NUM $WORKER_NUM 1 0 Clock OnDemand $DEFAULT_HCC_SIZE_LIMIT 1 WriteThrough NonPart GROUP_WAL $DEFAULT_WAL_GROUP_COMMIT_TIME 0 $TPCC_RUN_TIME $TPCC_WARMUP_TIME        # Tigon-TwoPL without phantom detection

########### Tigon End-to-End Performance END ###########
