#! /bin/bash

set -uo pipefail
# set -x

typeset SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

if [ $# != 1 ]; then
        echo "[Usage] ./plot_migration_policy.sh RES_DIR"
        exit -1
fi

typeset RES_DIR=$1

$SCRIPT_DIR/plot_tpcc_migration_policy.py $RES_DIR

$SCRIPT_DIR/plot_ycsb_migration_policy.py $RES_DIR 50 0
$SCRIPT_DIR/plot_ycsb_migration_policy.py $RES_DIR 50 0.99
$SCRIPT_DIR/plot_ycsb_migration_policy.py $RES_DIR 90 0
$SCRIPT_DIR/plot_ycsb_migration_policy.py $RES_DIR 90 0.99
