#! /bin/bash

# set -uo pipefail
# set -x

typeset SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
typeset current_date_time="`date +%Y%m%d%H%M`"

function print_usage {
        echo "[usage] ./setup.sh [deps/host/vm_image/kill_vms/launch_vms/vms] EXP-SPECIFIC"
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

        # tool chains
        sudo apt-get install -y cmake gcc-12 g++-12 clang-15 clang++-15 lld-15 cargo

        # libraries
        sudo apt-get install -y libboost-all-dev libjemalloc-dev libgoogle-glog-dev libgtest-dev

        # required by VM-based emulation
        sudo apt-get install -y python3 python3-pip mkosi ovmf numactl
        sudo pip3 install pyroute2

        # install Rust
        if ! which rustc; then
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y;
        source ~/.cargo/env
        fi
        source $HOME/.cargo/env

        exit 0
elif [ $TASK_TYPE = "host" ]; then
        if [ $# != 1 ]; then
                print_usage
                exit -1
        fi
        echo "Setting up current machine... Will reboot the machine"

        # setup ssh key
        [ -f $HOME/.ssh/id_rsa ] || ssh-keygen -t rsa -N "" -f $HOME/.ssh/id_rsa
        cat $HOME/.ssh/id_rsa.pub >> $HOME/.ssh/authorized_keys

        # kernel 5.19.0-50-generic
        sudo apt-get install -y linux-image-5.19.0-50-generic linux-headers-5.19.0-50-generic \
                linux-hwe-5.19-headers-5.19.0-50 linux-modules-5.19.0-50-generic \
                linux-modules-extra-5.19.0-50-generic

        # reboot to switch to the new kernel
        sudo reboot

        echo "should never reach here!"
        exit -1
elif [ $TASK_TYPE = "vm_image" ]; then
        if [ $# != 1 ]; then
                print_usage
                exit -1
        fi
        echo "Building VM image..."

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

        cd $SCRIPT_DIR/../dependencies/vhive_setup/pasha/

        if [ $HOST_TYPE = "chameleon" ]; then
                ./start_vm.sh --using-old-img --nosriov 0 5 $HOST_NUM 1 1
        elif [ $HOST_TYPE = "uiuc" ]; then
                ./start_vm_uiuc.sh --using-old-img --nosriov 0 5 $HOST_NUM 0 2
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

        # sync cxl_shmem
        echo "Sync cxl_shmem..."
        sync_files $SCRIPT_DIR/../dependencies/cxl_shmem/src/cxlalloc/target/release/cxl-init /root/cxl-init $HOST_NUM
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
