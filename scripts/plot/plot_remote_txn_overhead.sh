#! /bin/bash

set -uo pipefail
# set -x

typeset SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

if [ $# != 1 ]; then
        echo "[Usage] ./plot_remote_txn_overhead.sh RES_DIR"
        exit -1
fi

typeset RES_DIR=$1

$SCRIPT_DIR/plot_tpcc_remote_txn_overhead.py $RES_DIR NoOP
$SCRIPT_DIR/plot_tpcc_remote_txn_overhead.py $RES_DIR WriteThrough
$SCRIPT_DIR/plot_tpcc_remote_txn_overhead.py $RES_DIR NonTemporal


$SCRIPT_DIR/plot_ycsb_remote_txn_overhead.py $RES_DIR 0 NoOP
$SCRIPT_DIR/plot_ycsb_remote_txn_overhead.py $RES_DIR 0 WriteThrough
$SCRIPT_DIR/plot_ycsb_remote_txn_overhead.py $RES_DIR 0 NonTemporal

$SCRIPT_DIR/plot_ycsb_remote_txn_overhead.py $RES_DIR 0.99 NoOP
$SCRIPT_DIR/plot_ycsb_remote_txn_overhead.py $RES_DIR 0.99 WriteThrough
$SCRIPT_DIR/plot_ycsb_remote_txn_overhead.py $RES_DIR 0.99 NonTemporal
