#! /bin/bash

# set -uo pipefail
# set -x

typeset SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
typeset current_date_time="`date +%Y%m%d%H%M`"

function print_usage {
        echo "[usage] ./setup.sh [deps/cur_host/vm_image/kill_vms/launch_vms/vms] EXP-SPECIFIC"
        echo "deps: None"
        echo "cur_host: None"
        echo "vm_image: None"
        echo "kill_vms: None"
        echo "launch_vms: HOST_TYPE HOST_NUM"
        echo "vms: HOST_NUM"
}

if [ $# -lt 1 ]; then
        print_usage
        exit -1
fi

typeset TASK_TYPE=$1

source $SCRIPT_DIR/utilities.sh

if [ $TASK_TYPE = "deps" ]; then
        if [ $# != 1 ]; then
                print_usage
                exit -1
        fi
        echo "Setting up dependencies..."
        git submodule update --init --recursive

        # build cxl_shmem
        cd $SCRIPT_DIR/../dependencies/cxl_shmem
        task install_deps
        task check_driver
        task build_clang_release

        echo "Finished!"
        exit 0
elif [ $TASK_TYPE = "cur_host" ]; then
        if [ $# != 1 ]; then
                print_usage
                exit -1
        fi
        echo "Setting up current machine... Will reboot the machine later"
        git submodule update --init --recursive

        # setup current host
        cd $SCRIPT_DIR/../dependencies/vhive_setup/linux_qemu/
        ./setup_current_host.sh

        echo "should never reach here!"
        exit -1
elif [ $TASK_TYPE = "vm_image" ]; then
        if [ $# != 1 ]; then
                print_usage
                exit -1
        fi
        echo "Building VM image..."
        git submodule update --init --recursive

        cd $SCRIPT_DIR/../dependencies/vhive_setup/linux_qemu/setup_vm/
        ./make_vmimg.sh

        echo "Finished!"
        exit 0
elif [ $TASK_TYPE = "kill_vms" ]; then
        if [ $# != 1 ]; then
                print_usage
                exit -1
        fi
        echo "Killing VMs..."
        git submodule update --init --recursive

        # setup current host
        cd $SCRIPT_DIR/../dependencies/vhive_setup/pasha/
        ./kill_experiments.sh

        echo "Finished!"
        exit 0
elif [ $TASK_TYPE = "launch_vms" ]; then
        if [ $# != 3 ]; then
                print_usage
                exit -1
        fi
        typeset HOST_TYPE=$2
        typeset HOST_NUM=$3
        echo "Launching VMs..."
        git submodule update --init --recursive

        cd $SCRIPT_DIR/../dependencies/vhive_setup/pasha/

        if [ $HOST_TYPE = "chameleon" ]; then
                ./start_vm.sh --using-old-img --sriov 0 5 $HOST_NUM 1 1
        elif [ $HOST_TYPE = "uiuc" ]; then
                ./start_vm_uiuc.sh --using-old-img --sriov 0 5 $HOST_NUM 0 2
        fi

        echo "Finished!"
        exit 0
elif [ $TASK_TYPE = "vms" ]; then
        if [ $# != 2 ]; then
                print_usage
                exit -1
        fi
        typeset HOST_NUM=$2
        echo "Setting up VMs..."
        git submodule update --init --recursive

        # sync cxl_shmem
        echo "Sync cxl_shmem..."
        sync_files $SCRIPT_DIR/../dependencies/cxl_shmem/build_clang_release/cxl-init /root/cxl-init $HOST_NUM
        sync_files $SCRIPT_DIR/../dependencies/cxl_shmem/build_clang_release/bin/cxl_recover_meta /root/cxl_recover_meta $HOST_NUM
        sync_files $SCRIPT_DIR/../dependencies/cxl_shmem/driver_futex/jj_abortable_spin/byte/cxl_ivpci.ko /root/cxl_ivpci.ko $HOST_NUM

        # sync dependencies
        echo "Sync dependencies..."
        sync_files /lib/x86_64-linux-gnu/libjemalloc.so.2 /lib/x86_64-linux-gnu/libjemalloc.so.2 $HOST_NUM
        sync_files /lib/x86_64-linux-gnu/libjemalloc.so.2 /root/libjemalloc.so.2 $HOST_NUM
        sync_files /lib/x86_64-linux-gnu/libglog.so.0  /lib/x86_64-linux-gnu/libglog.so.0 $HOST_NUM
        sync_files /lib/x86_64-linux-gnu/libgflags.so.2.2 /lib/x86_64-linux-gnu/libgflags.so.2.2 $HOST_NUM

        # setup the VM(s)
        echo "Loading kernel module..."
        for (( i=0; i < $HOST_NUM; ++i ))
        do
                ssh_command "rmmod cxl_ivpci" $i
                ssh_command "insmod ./cxl_ivpci.ko" $i
        done

        echo "Finished"
        exit 0
else
        print_usage
        exit -1
fi
