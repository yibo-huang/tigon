#! /bin/bash

# set -uo pipefail
# set -x

typeset SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
typeset current_date_time="`date +%Y%m%d%H%M`"

if [ $# != 2 ]; then
        echo "[Usage] ./setup.sh [deps/baselines/vms] HOST_NUM"
        exit -1
fi

typeset TASK_TYPE=$1
typeset HOST_NUM=$2

source $SCRIPT_DIR/utilities.sh

if [ $TASK_TYPE = "deps" ]; then
        echo "Setting up dependencies..."

        # # build cxl_shmem
        # cd $SCRIPT_DIR/../deps/cxl_shmem
        # task install_deps
        # task check_driver
        # task build_clang_release

        echo "Finished!"
        exit 0

elif [ $TASK_TYPE = "vms" ]; then
        echo "Setting up VMs..."

        # # sync cxl_shmem
        # echo "Sync cxl_shmem..."
        # sync_files $SCRIPT_DIR/../deps/cxl_shmem/build_clang_release/cxl-init /root/cxl-init $HOST_NUM
        # sync_files $SCRIPT_DIR/../deps/cxl_shmem/build_clang_release/bin/cxl_recover_meta /root/cxl_recover_meta $HOST_NUM
        # sync_files $SCRIPT_DIR/../deps/cxl_shmem/driver_futex/jj_abortable_spin/byte/cxl_ivpci.ko /root/cxl_ivpci.ko $HOST_NUM

        # sync dependencies
        echo "Sync dependencies..."
        sync_files /lib/x86_64-linux-gnu/libjemalloc.so.2 /lib/x86_64-linux-gnu/libjemalloc.so.2 $HOST_NUM
        sync_files /lib/x86_64-linux-gnu/libjemalloc.so.2 /root/libjemalloc.so.2 $HOST_NUM
        sync_files /lib/x86_64-linux-gnu/libglog.so.0  /lib/x86_64-linux-gnu/libglog.so.0 $HOST_NUM
        sync_files /lib/x86_64-linux-gnu/libgflags.so.2.2 /lib/x86_64-linux-gnu/libgflags.so.2.2 $HOST_NUM

        # # setup the VM(s)
        # echo "Loading kernel module..."
        # for (( i=0; i < $HOST_NUM; ++i ))
        # do
        #         ssh_command "rmmod cxl_ivpci" $i
        #         ssh_command "insmod ./cxl_ivpci.ko" $i
        # done

        echo "Finished"
        exit 0
else
        echo "[Usage] ./setup.sh [deps/vms] HOST_NUM"
        exit -1
fi
