#! /bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

pkill qemu
python3 $SCRIPT_DIR/vm_lib/start_vm.py clean_ivshmem --shmem_dir=/mnt/cxl_mem
