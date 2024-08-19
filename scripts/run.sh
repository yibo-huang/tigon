#! /bin/bash

set -uo pipefail
# set -x

typeset SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
typeset current_date_time="`date +%Y%m%d%H%M`"

source $SCRIPT_DIR/utilities.sh

function print_usage {
        echo "[usage] ./run.sh [TPCC/YCSB/KILL/COMPILE_SYNC/CI] EXP-SPECIFIC"
        echo "TPCC: [SundialPasha/Sundial/TwoPL/Lotus/Calvin] HOST_NUM WORKER_NUM REMOTE_NEWORDER_PERC REMOTE_PAYMENT_PERC USE_CXL_TRANS CXL_TRANS_ENTRY_NUM GATHER_OUTPUTS"
        echo "YCSB: [SundialPasha/Sundial/TwoPL/Lotus/Calvin] HOST_NUM WORKER_NUM RW_RATIO ZIPF_THETA CROSS_RATIO USE_CXL_TRANS CXL_TRANS_ENTRY_NUM GATHER_OUTPUTS"
        echo "KILL: None"
        echo "COMPILE_SYNC: HOST_NUM"
        echo "CI: HOST_NUM"
}

function kill_prev_exps {
        typeset MAX_HOST_NUM=8
        typeset i=0

        echo "killing previous experiments..."
        for (( i=0; i < $MAX_HOST_NUM; ++i ))
        do
                ssh_command "pkill bench_tpcc" $i
                ssh_command "pkill bench_ycsb" $i
        done
}

function gather_other_output {
        typeset HOST_NUM=$1

        typeset i=0

        for (( i=1; i < $HOST_NUM; ++i ))
        do
                ssh_command "cat pasha/output.txt" $i
                ssh_command "rm pasha/output.txt" $i
        done
}

function sync_binaries {
        typeset HOST_NUM=$1

        cd $SCRIPT_DIR
        echo "Syncing executables and configs..."
        sync_files $SCRIPT_DIR/../build /root/pasha $HOST_NUM
}

function print_server_string {
        typeset HOST_NUM=$1

        typeset base=2
        typeset i=0

        for (( i=0; i < $HOST_NUM; ++i ))
        do
                typeset ip=$(expr $base + $i)
                if [ $i = 0 ]; then
                        echo -n "192.168.100.$ip:1234"
                else
                        echo -n ";192.168.100.$ip:1234"
                fi
        done
}

function run_exp_tpcc {
        typeset PROTOCOL=$1
        typeset HOST_NUM=$2
        typeset WORKER_NUM=$3
        typeset REMOTE_NEWORDER_PERC=$4
        typeset REMOTE_PAYMENT_PERC=$5
        typeset USE_CXL_TRANS=$6
        typeset CXL_TRANS_ENTRY_NUM=$7
        typeset GATHER_OUTPUTS=$8

        typeset PARTITION_NUM=$(expr $HOST_NUM \* $WORKER_NUM)
        typeset SERVER_STRING=$(print_server_string $HOST_NUM)
        typeset i=0

        kill_prev_exps
        init_cxl_for_vms $HOST_NUM

        if [ $PROTOCOL = "SundialPasha" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_tpcc --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0
                                --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --protocol=SundialPasha --query=mixed --neworder_dist=$REMOTE_NEWORDER_PERC --payment_dist=$REMOTE_PAYMENT_PERC &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_tpcc --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0
                        --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --protocol=SundialPasha --query=mixed --neworder_dist=$REMOTE_NEWORDER_PERC --payment_dist=$REMOTE_PAYMENT_PERC" 0

        elif [ $PROTOCOL = "Sundial" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_tpcc --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0
                                --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --protocol=Sundial --query=mixed --neworder_dist=$REMOTE_NEWORDER_PERC --payment_dist=$REMOTE_PAYMENT_PERC &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_tpcc --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0
                        --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --protocol=Sundial --query=mixed --neworder_dist=$REMOTE_NEWORDER_PERC --payment_dist=$REMOTE_PAYMENT_PERC" 0

        elif [ $PROTOCOL = "TwoPLPasha" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_tpcc --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0
                                --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --protocol=TwoPL --query=mixed --neworder_dist=$REMOTE_NEWORDER_PERC --payment_dist=$REMOTE_PAYMENT_PERC &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_tpcc --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0
                        --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --protocol=TwoPL --query=mixed --neworder_dist=$REMOTE_NEWORDER_PERC --payment_dist=$REMOTE_PAYMENT_PERC" 0

        elif [ $PROTOCOL = "TwoPL" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_tpcc --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0
                                --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --protocol=TwoPL --query=mixed --neworder_dist=$REMOTE_NEWORDER_PERC --payment_dist=$REMOTE_PAYMENT_PERC &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_tpcc --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0
                        --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --protocol=TwoPL --query=mixed --neworder_dist=$REMOTE_NEWORDER_PERC --payment_dist=$REMOTE_PAYMENT_PERC" 0

        elif [ $PROTOCOL = "Lotus" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_tpcc --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0
                                --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --protocol=HStore --query=mixed --neworder_dist=$REMOTE_NEWORDER_PERC --payment_dist=$REMOTE_PAYMENT_PERC &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_tpcc --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0
                        --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --protocol=HStore --query=mixed --neworder_dist=$REMOTE_NEWORDER_PERC --payment_dist=$REMOTE_PAYMENT_PERC" 0

        elif [ $PROTOCOL = "Calvin" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_tpcc --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=$HOST_NUM --lock_manager=1 --batch_flush=1 --lotus_async_repl=false --batch_size=20
                                --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --protocol=Calvin --query=mixed --neworder_dist=$REMOTE_NEWORDER_PERC --payment_dist=$REMOTE_PAYMENT_PERC &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_tpcc --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=$HOST_NUM --lock_manager=1 --batch_flush=1 --lotus_async_repl=false --batch_size=20
                        --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --protocol=Calvin --query=mixed --neworder_dist=$REMOTE_NEWORDER_PERC --payment_dist=$REMOTE_PAYMENT_PERC" 0

        else
                echo "Protocol not supported!"
                exit -1
        fi

        if [ $GATHER_OUTPUT == 1 ]; then
                gather_other_output $HOST_NUM
        fi

        kill_prev_exps
}

function run_exp_ycsb {
        typeset PROTOCOL=$1
        typeset HOST_NUM=$2
        typeset WORKER_NUM=$3
        typeset RW_RATIO=$4
        typeset ZIPF_THETA=$5
        typeset CROSS_RATIO=$6
        typeset USE_CXL_TRANS=$7
        typeset CXL_TRANS_ENTRY_NUM=$8
        typeset GATHER_OUTPUT=$9

        typeset PARTITION_NUM=$(expr $HOST_NUM \* $WORKER_NUM)
        typeset SERVER_STRING=$(print_server_string $HOST_NUM)
        typeset i=0

        kill_prev_exps
        init_cxl_for_vms $HOST_NUM

        if [ $PROTOCOL = "SundialPasha" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_ycsb --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0
                                --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --protocol=SundialPasha --keys=100000 --read_write_ratio=$RW_RATIO --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO --cross_part_num=2 &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_ycsb --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0
                        --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --protocol=SundialPasha --keys=100000 --read_write_ratio=$RW_RATIO --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO --cross_part_num=2" 0

        elif [ $PROTOCOL = "Sundial" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_ycsb --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0
                                --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --protocol=Sundial --keys=100000 --read_write_ratio=$RW_RATIO --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO --cross_part_num=2 &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_ycsb --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0
                        --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --protocol=Sundial --keys=100000 --read_write_ratio=$RW_RATIO --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO --cross_part_num=2" 0

        elif [ $PROTOCOL = "TwoPLPasha" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_ycsb --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0
                                --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --protocol=TwoPLPasha --keys=100000 --read_write_ratio=$RW_RATIO --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO --cross_part_num=2 &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_ycsb --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0
                        --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --protocol=TwoPLPasha --keys=100000 --read_write_ratio=$RW_RATIO --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO --cross_part_num=2" 0

        elif [ $PROTOCOL = "TwoPL" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_ycsb --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0
                                --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --protocol=TwoPL --keys=100000 --read_write_ratio=$RW_RATIO --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO --cross_part_num=2 &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_ycsb --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0
                        --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --protocol=TwoPL --keys=100000 --read_write_ratio=$RW_RATIO --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO --cross_part_num=2" 0

        elif [ $PROTOCOL = "Lotus" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_ycsb --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0
                                --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --protocol=HStore --keys=100000 --read_write_ratio=$RW_RATIO --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO --cross_part_num=2 &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_ycsb --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0
                        --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --protocol=HStore --keys=100000 --read_write_ratio=$RW_RATIO --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO --cross_part_num=2" 0

        elif [ $PROTOCOL = "Calvin" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_ycsb --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=$HOST_NUM --lock_manager=1 --batch_flush=1 --lotus_async_repl=false --batch_size=1200
                                --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --protocol=Calvin --keys=100000 --read_write_ratio=$RW_RATIO --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO --cross_part_num=2 &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_ycsb --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=$HOST_NUM --lock_manager=1 --batch_flush=1 --lotus_async_repl=false --batch_size=1200
                        --use_cxl_transport=$USE_CXL_TRANS --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --protocol=Calvin --keys=100000 --read_write_ratio=$RW_RATIO --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO --cross_part_num=2" 0

        else
                echo "Protocol not supported!"
                exit -1
        fi

        if [ $GATHER_OUTPUT == 1 ]; then
                gather_other_output $HOST_NUM
        fi

        kill_prev_exps
}

# process arguments
if [ $# -le 1 ]; then
        print_usage
        exit -1
fi

typeset RUN_TYPE=$1

if [ $RUN_TYPE = "TPCC" ]; then
        if [ $# != 9 ]; then
                print_usage
                exit -1
        fi

        typeset PROTOCOL=$2
        typeset HOST_NUM=$3
        typeset WORKER_NUM=$4
        typeset REMOTE_NEWORDER_PERC=$5
        typeset REMOTE_PAYMENT_PERC=$6
        typeset USE_CXL_TRANS=$7
        typeset CXL_TRANS_ENTRY_NUM=$8
        typeset GATHER_OUTPUT=$9

        run_exp_tpcc $PROTOCOL $HOST_NUM $WORKER_NUM $REMOTE_NEWORDER_PERC $REMOTE_PAYMENT_PERC $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM $GATHER_OUTPUT
        exit 0
elif [ $RUN_TYPE = "YCSB" ]; then
        if [ $# != 10 ]; then
                print_usage
                exit -1
        fi

        typeset PROTOCOL=$2
        typeset HOST_NUM=$3
        typeset WORKER_NUM=$4
        typeset RW_RATIO=$5
        typeset ZIPF_THETA=$6
        typeset CROSS_RATIO=$7
        typeset USE_CXL_TRANS=$8
        typeset CXL_TRANS_ENTRY_NUM=$9
        typeset GATHER_OUTPUT=${10}

        run_exp_ycsb $PROTOCOL $HOST_NUM $WORKER_NUM $RW_RATIO $ZIPF_THETA $CROSS_RATIO $USE_CXL_TRANS $CXL_TRANS_ENTRY_NUM $GATHER_OUTPUT
        exit 0
elif [ $RUN_TYPE = "KILL" ]; then
        if [ $# != 2 ]; then
                print_usage
                exit -1
        fi

        typeset HOST_NUM=$2

        kill_prev_exps $HOST_NUM

        exit 0
elif [ $RUN_TYPE = "COMPILE_SYNC" ]; then
        if [ $# != 2 ]; then
                print_usage
                exit -1
        fi

        typeset HOST_NUM=$2

        # compile
        cd $SCRIPT_DIR/../
        mkdir -p build
        cd build
        cmake ..
        make -j

        # sync
        sync_binaries $HOST_NUM

        exit 0
elif [ $RUN_TYPE = "CI" ]; then
        if [ $# != 2 ]; then
                print_usage
                exit -1
        fi

        typeset HOST_NUM=$2

        run_exp_tpcc SundialPasha $HOST_NUM 2 10 15 true 4096
        run_exp_tpcc Sundial $HOST_NUM 2 10 15 true 4096
        run_exp_tpcc Lotus $HOST_NUM 2 10 15 true 4096
        run_exp_tpcc Calvin $HOST_NUM 2 10 15 true 4096

        run_exp_tpcc SundialPasha $HOST_NUM 2 10 15 true 4096
        run_exp_ycsb Sundial $HOST_NUM 2 50 0 10 true 4096
        run_exp_ycsb Lotus $HOST_NUM 2 50 0 10 true 4096
        run_exp_ycsb Calvin $HOST_NUM 2 50 0 10 true 4096

        exit 0
else
        print_usage
        exit -1
fi
