#! /bin/bash

set -uo pipefail
# set -x

typeset SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
typeset current_date_time="`date +%Y%m%d%H%M`"

function print_usage {
        echo "[usage] $0 RESULT_ROOT_DIR"
}

if [ $# != 1 ]; then
        print_usage
        exit -1
fi

typeset RESULT_ROOT_DIR=$1

# run experiments
./scripts/run_tpcc.sh $RESULT_ROOT_DIR
./scripts/run_ycsb.sh $RESULT_ROOT_DIR
./scripts/run_hwcc_budget.sh $RESULT_ROOT_DIR
./scripts/run_swcc.sh $RESULT_ROOT_DIR

# parse and generate Figure 5
./scripts/parse/parse_tpcc.py $RESULT_ROOT_DIR
./scripts/plot/plot_tpcc_sundial.py $RESULT_ROOT_DIR
./scripts/plot/plot_tpcc_twopl.py $RESULT_ROOT_DIR
./scripts/plot/plot_tpcc.py $RESULT_ROOT_DIR

# parse and generate Figure 6
./scripts/parse/parse_ycsb.py $RESULT_ROOT_DIR
./scripts/plot/plot_ycsb.py $RESULT_ROOT_DIR

# parse and generate Figure 7
./scripts/parse/parse_hwcc_budget.py $RESULT_ROOT_DIR
./scripts/plot/plot_hwcc_budget.py $RESULT_ROOT_DIR

# parse and generate Figure 8
./scripts/parse/parse_swcc.py $RESULT_ROOT_DIR
./scripts/plot/plot_swcc.py $RESULT_ROOT_DIR
