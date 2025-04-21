#!/bin/bash

set -euo pipefail
set -x
typeset SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

if [ $# != 7 ]; then
    echo "[ERROR] Wrong arguments number. Abort."
    exit 0
fi

# $1 = --using-new-or-old-img
# $2 = --cxl or --sriov
typeset host_id=$3
typeset num_cpus=$4
typeset num_vms=$5
typeset configure_uncore_freq=$6
typeset shmem_dir_numa=$7

# configure UNCORE frequency and HyperThreading
source $SCRIPT_DIR/host_setup/cxl_global.sh
if [ $configure_uncore_freq -eq 1 ]; then
    enable_ht
    sudo modprobe msr
    sudo python3 $SCRIPT_DIR/host_setup/uncore_freq.py set --freq 2400 --socket 0
    sudo python3 $SCRIPT_DIR/host_setup/uncore_freq.py set --freq 0 --socket 1
    sudo python3 $SCRIPT_DIR/host_setup/uncore_freq.py print
    cd $SCRIPT_DIR
fi
check_comm_conf_no_tune_freq

if [ $1 = "--using-new-img" ]; then
    echo "starting vm using new img..."
    test -e $SCRIPT_DIR/vms && rm -r $SCRIPT_DIR/vms
else
    echo "starting vm using existing img..."
fi

mkdir -p $SCRIPT_DIR/vms

if [ $2 = "--sriov" ]; then
        sudo python3 $SCRIPT_DIR/vm_lib/start_vm.py start_vm --num_vms $num_vms \
        --num_cpus $num_cpus --mem_size_mb 10240 \
        --shmem_dir /mnt/cxl_mem --shmem_size_mb 65536 --vmdir $SCRIPT_DIR/vms \
        --use_mlnx_ether --num_ether_per_vm 1 --num_ib_per_vm 0 --add_user_ssh --use-ivshmem-doorbell --host_id $host_id \
        --shmem_dir_numa $shmem_dir_numa
else
        sudo python3 $SCRIPT_DIR/vm_lib/start_vm.py start_vm --num_vms $num_vms \
        --num_cpus $num_cpus --mem_size_mb 10240 \
        --shmem_dir /mnt/cxl_mem --shmem_size_mb 65536 --vmdir $SCRIPT_DIR/vms \
        --add_user_ssh --use-ivshmem-doorbell --host_id $host_id \
        --shmem_dir_numa $shmem_dir_numa
fi
