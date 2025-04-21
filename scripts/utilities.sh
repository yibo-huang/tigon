#! /bin/bash

set -uo pipefail

# set -x

function ssh_command {
        typeset command=$1
        typeset vm_id=$2
        typeset base_port=10022

        typeset port=$(expr $base_port + $vm_id)
        ssh -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -p $port root@127.0.0.1 ""$command""
}

function sync_files {
        typeset src=$1
        typeset dst=$2
        typeset vm_num=$3
        typeset base_port=10022
        typeset i=0

        for (( i=0; i < $vm_num; ++i ))
        do
                echo ......syncing to VM $i......
                port=$(expr $base_port + $i)
                scp -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -r -P $port $src root@127.0.0.1:$dst > /dev/null
        done
}

function setup_hostnames {
        typeset vm_num=$1
        typeset base=2
        typeset i=0

        for (( i=0; i < $vm_num; ++i ))
        do
                echo ......setup hostname for VM $i......
                ip=$(expr $base + $i)
                ssh_command "hostnamectl set-hostname 192.168.100.$ip" $i
        done
}

function init_cxl_for_vms {
        typeset vm_num=$1
        typeset cxl_init=./cxl_init
        typeset i=0

        echo initializing cxl memory...
        ssh_command "$cxl_init --machine-count 16 --size $((2 ** 30 * 64)) -z" 0
        for (( i=1; i < $vm_num; ++i ))
        do
                ssh_command "./cxl_recover_meta --tot_machines 16" $i > /dev/null
        done
}

function rm_files_for_vms {
        typeset file=$1
        typeset vm_num=$2
        typeset i=0
        for (( i=0; i < $vm_num; ++i ))
	do
		ssh_command "[ -e $file ] && rm -rf $file" $i
	done
}
