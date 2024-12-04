#! /bin/bash

# set -uo pipefail
# set -x

typeset SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
typeset current_date_time="`date +%Y%m%d%H%M`"

if [ $# != 2 ]; then
        echo "[Usage] ./plot_all.sh PASHA_RES_DIR BASELINE_RES_DIR"
        exit -1
fi

typeset PASHA_RES_DIR=$1
typeset BASELINE_RES_DIR=$2

# Microbenchmark
$SCRIPT_DIR/plot_ycsb_with_read_cxl.py $PASHA_RES_DIR/micro 100 0.7
$SCRIPT_DIR/plot_ycsb.py $PASHA_RES_DIR/micro 100 0.7
$SCRIPT_DIR/plot_ycsb.py $PASHA_RES_DIR/micro 0 0.7
$SCRIPT_DIR/plot_ycsb_custom.py $PASHA_RES_DIR/micro 0.7

# End-to-End performance
$SCRIPT_DIR/plot_tpcc.py $PASHA_RES_DIR/macro
$SCRIPT_DIR/plot_ycsb.py $PASHA_RES_DIR/macro 95 0.7
$SCRIPT_DIR/plot_ycsb.py $PASHA_RES_DIR/macro 50 0.7

# Shortcut
$SCRIPT_DIR/plot_tpcc_shortcut.py $PASHA_RES_DIR/shortcut
$SCRIPT_DIR/plot_ycsb_shortcut.py $PASHA_RES_DIR/shortcut 95 0.7

# Data Movement
$SCRIPT_DIR/plot_tpcc_data_movement.py $PASHA_RES_DIR/data-movement
