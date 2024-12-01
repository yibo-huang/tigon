#! /bin/bash

set -uo pipefail
# set -x

typeset SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
typeset current_date_time="`date +%Y%m%d%H%M`"

source $SCRIPT_DIR/utilities.sh

function print_usage {
        echo "[usage] ./run.sh [TPCC/YCSB/KILL/COMPILE/COMPILE_SYNC/CI/COLLECT_OUTPUTS] EXP-SPECIFIC"
        echo "TPCC: [SundialPasha/Sundial/TwoPLPasha/TwoPL] HOST_NUM WORKER_NUM QUERY_TYPE REMOTE_NEWORDER_PERC REMOTE_PAYMENT_PERC USE_CXL_TRANS USE_OUTPUT_THREAD MIGRATION_POLICY WHEN_TO_MOVE_OUT HW_CC_BUDGET SCC_MECH PRE_MIGRATE TIME_TO_RUN TIME_TO_WARMUP LOGGING_TYPE GATHER_OUTPUTS"
        echo "YCSB: [SundialPasha/Sundial/TwoPLPasha/TwoPL] HOST_NUM WORKER_NUM QUERY_TYPE KEYS RW_RATIO ZIPF_THETA CROSS_RATIO USE_CXL_TRANS USE_OUTPUT_THREAD MIGRATION_POLICY WHEN_TO_MOVE_OUT HW_CC_BUDGET SCC_MECH PRE_MIGRATE TIME_TO_RUN TIME_TO_WARMUP LOGGING_TYPE GATHER_OUTPUTS"
        echo "SmallBank: [SundialPasha/Sundial/TwoPLPasha/TwoPL] HOST_NUM WORKER_NUM KEYS ZIPF_THETA CROSS_RATIO USE_CXL_TRANS USE_OUTPUT_THREAD MIGRATION_POLICY WHEN_TO_MOVE_OUT HW_CC_BUDGET SCC_MECH PRE_MIGRATE TIME_TO_RUN TIME_TO_WARMUP LOGGING_TYPE GATHER_OUTPUTS"
        echo "TATP: [SundialPasha/Sundial/TwoPLPasha/TwoPL] HOST_NUM WORKER_NUM KEYS CROSS_RATIO USE_CXL_TRANS USE_OUTPUT_THREAD MIGRATION_POLICY WHEN_TO_MOVE_OUT HW_CC_BUDGET SCC_MECH PRE_MIGRATE TIME_TO_RUN TIME_TO_WARMUP LOGGING_TYPE GATHER_OUTPUTS"
        echo "KILL: None"
        echo "COMPILE: None"
        echo "COMPILE_SYNC: HOST_NUM"
        echo "CI: HOST_NUM WORKER_NUM"
        echo "COLLECT_OUTPUTS: HOST_NUM"
}

function kill_prev_exps {
        typeset MAX_HOST_NUM=8
        typeset i=0

        echo "killing previous experiments..."
        for (( i=0; i < $MAX_HOST_NUM; ++i ))
        do
                ssh_command "pkill bench_tpcc" $i
                ssh_command "pkill bench_ycsb" $i
                # ssh_command "pkill bench_smallbank" $i
                # ssh_command "pkill bench_tatp" $i
        done
}

function delete_log_files {
        typeset MAX_HOST_NUM=8
        typeset LOG_FILE_NAME=pasha_log_non_group_commit.txt
        typeset i=0

        echo "deleting log files..."
        # for (( i=0; i < $MAX_HOST_NUM; ++i ))
        # do
        #         ssh_command "[ -e $LOG_FILE_NAME ] && rm $LOG_FILE_NAME" $i
        #         ssh_command "echo 1 > /proc/sys/vm/drop_caches" $i
        #         ssh_command "sync" $i
        # done
}

function gather_other_output {
        typeset HOST_NUM=$1

        typeset i=0

        for (( i=1; i < $HOST_NUM; ++i ))
        do
                ssh_command "cat pasha/output.txt" $i
        done
}

function sync_binaries {
        typeset HOST_NUM=$1

        cd $SCRIPT_DIR
        echo "Syncing executables and configs..."
        for (( i=0; i < $HOST_NUM; ++i ))
        do
                ssh_command "mkdir -p pasha" $i
        done
        sync_files $SCRIPT_DIR/../build/bench_tpcc /root/pasha/ $HOST_NUM
        sync_files $SCRIPT_DIR/../build/bench_ycsb /root/pasha/ $HOST_NUM
        # sync_files $SCRIPT_DIR/../build/bench_smallbank /root/pasha/ $HOST_NUM
        # sync_files $SCRIPT_DIR/../build/bench_tatp /root/pasha/ $HOST_NUM
        exit -1
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
        if [ $# != 22 ]; then
                print_usage
                exit -1
        fi
        typeset PROTOCOL=$1
        typeset HOST_NUM=$2
        typeset WORKER_NUM=$3
        typeset QUERY_TYPE=$4
        typeset REMOTE_NEWORDER_PERC=$5
        typeset REMOTE_PAYMENT_PERC=$6
        typeset USE_CXL_TRANS=$7
        typeset USE_OUTPUT_THREAD=$8
        typeset CXL_TRANS_ENTRY_STRUCT_SIZE=$9
        typeset CXL_TRANS_ENTRY_NUM=${10}
        typeset MIGRATION_POLICY=${11}
        typeset WHEN_TO_MOVE_OUT=${12}
        typeset HW_CC_BUDGET=${13}
        typeset SCC_MECH=${14}
        typeset PRE_MIGRATE=${15}
        typeset TIME_TO_RUN=${16}
        typeset TIME_TO_WARMUP=${17}
        typeset LOG_PATH=${18}
        typeset LOTUS_CHECKPOINT=${19}
        typeset WAL_GROUP_COMMIT_TIME=${20}
        typeset WAL_GROUP_COMMIT_BATCH_SIZE=${21}
        typeset GATHER_OUTPUT=${22}

        typeset PARTITION_NUM=$(expr $HOST_NUM \* $WORKER_NUM)
        typeset SERVER_STRING=$(print_server_string $HOST_NUM)
        typeset i=0

        kill_prev_exps
        delete_log_files
        init_cxl_for_vms $HOST_NUM

        if [ $PROTOCOL = "SundialPasha" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_tpcc --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                                --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --migration_policy=$MIGRATION_POLICY --when_to_move_out=$WHEN_TO_MOVE_OUT --hw_cc_budget=$HW_CC_BUDGET
                                --scc_mechanism=$SCC_MECH
                                --pre_migrate=$PRE_MIGRATE
                                --protocol=SundialPasha --query=$QUERY_TYPE --neworder_dist=$REMOTE_NEWORDER_PERC --payment_dist=$REMOTE_PAYMENT_PERC &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_tpcc --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                        --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --migration_policy=$MIGRATION_POLICY --when_to_move_out=$WHEN_TO_MOVE_OUT --hw_cc_budget=$HW_CC_BUDGET
                        --scc_mechanism=$SCC_MECH
                        --pre_migrate=$PRE_MIGRATE
                        --protocol=SundialPasha --query=$QUERY_TYPE --neworder_dist=$REMOTE_NEWORDER_PERC --payment_dist=$REMOTE_PAYMENT_PERC" 0

        elif [ $PROTOCOL = "Sundial" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_tpcc --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                                --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --protocol=Sundial --query=$QUERY_TYPE --neworder_dist=$REMOTE_NEWORDER_PERC --payment_dist=$REMOTE_PAYMENT_PERC &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_tpcc --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                        --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --protocol=Sundial --query=$QUERY_TYPE --neworder_dist=$REMOTE_NEWORDER_PERC --payment_dist=$REMOTE_PAYMENT_PERC" 0

        elif [ $PROTOCOL = "TwoPLPasha" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_tpcc --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                                --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --migration_policy=$MIGRATION_POLICY --when_to_move_out=$WHEN_TO_MOVE_OUT --hw_cc_budget=$HW_CC_BUDGET
                                --scc_mechanism=$SCC_MECH
                                --pre_migrate=$PRE_MIGRATE
                                --protocol=TwoPLPasha --query=$QUERY_TYPE --neworder_dist=$REMOTE_NEWORDER_PERC --payment_dist=$REMOTE_PAYMENT_PERC &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_tpcc --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                        --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --migration_policy=$MIGRATION_POLICY --when_to_move_out=$WHEN_TO_MOVE_OUT --hw_cc_budget=$HW_CC_BUDGET
                        --scc_mechanism=$SCC_MECH
                        --pre_migrate=$PRE_MIGRATE
                        --protocol=TwoPLPasha --query=$QUERY_TYPE --neworder_dist=$REMOTE_NEWORDER_PERC --payment_dist=$REMOTE_PAYMENT_PERC" 0

        elif [ $PROTOCOL = "TwoPL" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_tpcc --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                                --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --protocol=TwoPL --query=$QUERY_TYPE --neworder_dist=$REMOTE_NEWORDER_PERC --payment_dist=$REMOTE_PAYMENT_PERC &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_tpcc --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                        --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --protocol=TwoPL --query=$QUERY_TYPE --neworder_dist=$REMOTE_NEWORDER_PERC --payment_dist=$REMOTE_PAYMENT_PERC" 0
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
        if [ $# != 24 ]; then
                print_usage
                exit -1
        fi
        typeset PROTOCOL=$1
        typeset HOST_NUM=$2
        typeset WORKER_NUM=$3
        typeset QUERY_TYPE=$4
        typeset KEYS=$5
        typeset RW_RATIO=$6
        typeset ZIPF_THETA=$7
        typeset CROSS_RATIO=$8
        typeset USE_CXL_TRANS=$9
        typeset USE_OUTPUT_THREAD=${10}
        typeset CXL_TRANS_ENTRY_STRUCT_SIZE=${11}
        typeset CXL_TRANS_ENTRY_NUM=${12}
        typeset MIGRATION_POLICY=${13}
        typeset WHEN_TO_MOVE_OUT=${14}
        typeset HW_CC_BUDGET=${15}
        typeset SCC_MECH=${16}
        typeset PRE_MIGRATE=${17}
        typeset TIME_TO_RUN=${18}
        typeset TIME_TO_WARMUP=${19}
        typeset LOG_PATH=${20}
        typeset LOTUS_CHECKPOINT=${21}
        typeset WAL_GROUP_COMMIT_TIME=${22}
        typeset WAL_GROUP_COMMIT_BATCH_SIZE=${23}
        typeset GATHER_OUTPUT=${24}

        typeset PARTITION_NUM=$(expr $HOST_NUM \* $WORKER_NUM)
        typeset SERVER_STRING=$(print_server_string $HOST_NUM)
        typeset i=0

        kill_prev_exps
        delete_log_files
        init_cxl_for_vms $HOST_NUM

        if [ $PROTOCOL = "SundialPasha" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_ycsb --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                                --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --migration_policy=$MIGRATION_POLICY --when_to_move_out=$WHEN_TO_MOVE_OUT --hw_cc_budget=$HW_CC_BUDGET
                                --scc_mechanism=$SCC_MECH
                                --pre_migrate=$PRE_MIGRATE
                                --protocol=SundialPasha --query=$QUERY_TYPE --keys=$KEYS --read_write_ratio=$RW_RATIO --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO --cross_part_num=2 &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_ycsb --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                        --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --migration_policy=$MIGRATION_POLICY --when_to_move_out=$WHEN_TO_MOVE_OUT --hw_cc_budget=$HW_CC_BUDGET
                        --scc_mechanism=$SCC_MECH
                        --pre_migrate=$PRE_MIGRATE
                        --protocol=SundialPasha --query=$QUERY_TYPE --keys=$KEYS --read_write_ratio=$RW_RATIO --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO --cross_part_num=2" 0

        elif [ $PROTOCOL = "Sundial" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_ycsb --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                                --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --protocol=Sundial --query=$QUERY_TYPE --keys=$KEYS --read_write_ratio=$RW_RATIO --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO --cross_part_num=2 &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_ycsb --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                        --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --protocol=Sundial --query=$QUERY_TYPE --keys=$KEYS --read_write_ratio=$RW_RATIO --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO --cross_part_num=2" 0

        elif [ $PROTOCOL = "TwoPLPasha" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_ycsb --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                                --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --migration_policy=$MIGRATION_POLICY --when_to_move_out=$WHEN_TO_MOVE_OUT --hw_cc_budget=$HW_CC_BUDGET
                                --scc_mechanism=$SCC_MECH
                                --pre_migrate=$PRE_MIGRATE
                                --protocol=TwoPLPasha --query=$QUERY_TYPE --keys=$KEYS --read_write_ratio=$RW_RATIO --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO --cross_part_num=2 &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_ycsb --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                        --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --migration_policy=$MIGRATION_POLICY --when_to_move_out=$WHEN_TO_MOVE_OUT --hw_cc_budget=$HW_CC_BUDGET
                        --scc_mechanism=$SCC_MECH
                        --pre_migrate=$PRE_MIGRATE
                        --protocol=TwoPLPasha --query=$QUERY_TYPE --keys=$KEYS --read_write_ratio=$RW_RATIO --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO --cross_part_num=2" 0

        elif [ $PROTOCOL = "TwoPL" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_ycsb --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                                --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --protocol=TwoPL --query=$QUERY_TYPE --keys=$KEYS --read_write_ratio=$RW_RATIO --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO --cross_part_num=2 &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_ycsb --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                        --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --protocol=TwoPL --query=$QUERY_TYPE --keys=$KEYS --read_write_ratio=$RW_RATIO --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO --cross_part_num=2" 0
        else
                echo "Protocol not supported!"
                exit -1
        fi

        if [ $GATHER_OUTPUT == 1 ]; then
                gather_other_output $HOST_NUM
        fi

        kill_prev_exps
}

function run_exp_smallbank {
        if [ $# != 22 ]; then
                print_usage
                exit -1
        fi
        typeset PROTOCOL=$1
        typeset HOST_NUM=$2
        typeset WORKER_NUM=$3
        typeset KEYS=$4
        typeset ZIPF_THETA=$5
        typeset CROSS_RATIO=$6
        typeset USE_CXL_TRANS=$7
        typeset USE_OUTPUT_THREAD=$8
        typeset CXL_TRANS_ENTRY_STRUCT_SIZE=$9
        typeset CXL_TRANS_ENTRY_NUM=${10}
        typeset MIGRATION_POLICY=${11}
        typeset WHEN_TO_MOVE_OUT=${12}
        typeset HW_CC_BUDGET=${13}
        typeset SCC_MECH=${14}
        typeset PRE_MIGRATE=${15}
        typeset TIME_TO_RUN=${16}
        typeset TIME_TO_WARMUP=${17}
        typeset LOG_PATH=${18}
        typeset LOTUS_CHECKPOINT=${19}
        typeset WAL_GROUP_COMMIT_TIME=${20}
        typeset WAL_GROUP_COMMIT_BATCH_SIZE=${21}
        typeset GATHER_OUTPUT=${22}

        typeset PARTITION_NUM=$(expr $HOST_NUM \* $WORKER_NUM)
        typeset SERVER_STRING=$(print_server_string $HOST_NUM)
        typeset i=0

        kill_prev_exps
        delete_log_files
        init_cxl_for_vms $HOST_NUM

        if [ $PROTOCOL = "SundialPasha" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_smallbank --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                                --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --migration_policy=$MIGRATION_POLICY --when_to_move_out=$WHEN_TO_MOVE_OUT --hw_cc_budget=$HW_CC_BUDGET
                                --scc_mechanism=$SCC_MECH
                                --pre_migrate=$PRE_MIGRATE
                                --protocol=SundialPasha --keys=$KEYS --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_smallbank --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                        --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --migration_policy=$MIGRATION_POLICY --when_to_move_out=$WHEN_TO_MOVE_OUT --hw_cc_budget=$HW_CC_BUDGET
                        --scc_mechanism=$SCC_MECH
                        --pre_migrate=$PRE_MIGRATE
                        --protocol=SundialPasha --keys=$KEYS --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO" 0

        elif [ $PROTOCOL = "Sundial" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_smallbank --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                                --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --protocol=Sundial --keys=$KEYS --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_smallbank --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                        --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --protocol=Sundial --keys=$KEYS --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO" 0

        elif [ $PROTOCOL = "TwoPLPasha" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_smallbank --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                                --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --migration_policy=$MIGRATION_POLICY --when_to_move_out=$WHEN_TO_MOVE_OUT --hw_cc_budget=$HW_CC_BUDGET
                                --scc_mechanism=$SCC_MECH
                                --pre_migrate=$PRE_MIGRATE
                                --protocol=TwoPLPasha --keys=$KEYS --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_smallbank --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                        --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --migration_policy=$MIGRATION_POLICY --when_to_move_out=$WHEN_TO_MOVE_OUT --hw_cc_budget=$HW_CC_BUDGET
                        --scc_mechanism=$SCC_MECH
                        --pre_migrate=$PRE_MIGRATE
                        --protocol=TwoPLPasha --keys=$KEYS --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO" 0

        elif [ $PROTOCOL = "TwoPL" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_smallbank --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                                --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --protocol=TwoPL --keys=$KEYS --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_smallbank --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                        --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --protocol=TwoPL --keys=$KEYS --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO" 0
        else
                echo "Protocol not supported!"
                exit -1
        fi

        if [ $GATHER_OUTPUT == 1 ]; then
                gather_other_output $HOST_NUM
        fi

        kill_prev_exps
}

function run_exp_tatp {
        if [ $# != 22 ]; then
                print_usage
                exit -1
        fi
        typeset PROTOCOL=$1
        typeset HOST_NUM=$2
        typeset WORKER_NUM=$3
        typeset KEYS=$4
        typeset ZIPF_THETA=$5
        typeset CROSS_RATIO=$6
        typeset USE_CXL_TRANS=$7
        typeset USE_OUTPUT_THREAD=$8
        typeset CXL_TRANS_ENTRY_STRUCT_SIZE=$9
        typeset CXL_TRANS_ENTRY_NUM=${10}
        typeset MIGRATION_POLICY=${11}
        typeset WHEN_TO_MOVE_OUT=${12}
        typeset HW_CC_BUDGET=${13}
        typeset SCC_MECH=${14}
        typeset PRE_MIGRATE=${15}
        typeset TIME_TO_RUN=${16}
        typeset TIME_TO_WARMUP=${17}
        typeset LOG_PATH=${18}
        typeset LOTUS_CHECKPOINT=${19}
        typeset WAL_GROUP_COMMIT_TIME=${20}
        typeset WAL_GROUP_COMMIT_BATCH_SIZE=${21}
        typeset GATHER_OUTPUT=${22}

        typeset PARTITION_NUM=$(expr $HOST_NUM \* 1)    # one partition per host
        typeset SERVER_STRING=$(print_server_string $HOST_NUM)
        typeset i=0

        kill_prev_exps
        delete_log_files
        init_cxl_for_vms $HOST_NUM

        if [ $PROTOCOL = "SundialPasha" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_tatp --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                                --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --migration_policy=$MIGRATION_POLICY --when_to_move_out=$WHEN_TO_MOVE_OUT --hw_cc_budget=$HW_CC_BUDGET
                                --scc_mechanism=$SCC_MECH
                                --pre_migrate=$PRE_MIGRATE
                                --protocol=SundialPasha --keys=$KEYS --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_tatp --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                        --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --migration_policy=$MIGRATION_POLICY --when_to_move_out=$WHEN_TO_MOVE_OUT --hw_cc_budget=$HW_CC_BUDGET
                        --scc_mechanism=$SCC_MECH
                        --pre_migrate=$PRE_MIGRATE
                        --protocol=SundialPasha --keys=$KEYS --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO" 0

        elif [ $PROTOCOL = "Sundial" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_tatp --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                                --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --protocol=Sundial --keys=$KEYS --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_tatp --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                        --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --protocol=Sundial --keys=$KEYS --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO" 0

        elif [ $PROTOCOL = "TwoPLPasha" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_tatp --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                                --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --migration_policy=$MIGRATION_POLICY --when_to_move_out=$WHEN_TO_MOVE_OUT --hw_cc_budget=$HW_CC_BUDGET
                                --scc_mechanism=$SCC_MECH
                                --pre_migrate=$PRE_MIGRATE
                                --protocol=TwoPLPasha --keys=$KEYS --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_tatp --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                        --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --migration_policy=$MIGRATION_POLICY --when_to_move_out=$WHEN_TO_MOVE_OUT --hw_cc_budget=$HW_CC_BUDGET
                        --scc_mechanism=$SCC_MECH
                        --pre_migrate=$PRE_MIGRATE
                        --protocol=TwoPLPasha --keys=$KEYS --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO" 0

        elif [ $PROTOCOL = "TwoPL" ]; then
                # launch 1-$HOST_NUM processes
                for (( i=1; i < $HOST_NUM; ++i ))
                do
                        ssh_command "cd pasha; nohup ./bench_tatp --logtostderr=1 --id=$i --servers=\"$SERVER_STRING\"
                                --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                                --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                                --partitioner=hash --hstore_command_logging=false
                                --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                                --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                                --protocol=TwoPL --keys=$KEYS --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO &> output.txt < /dev/null &" $i
                done

                # launch the first process
                ssh_command "cd pasha; ./bench_tatp --logtostderr=1 --id=0 --servers=\"$SERVER_STRING\"
                        --threads=$WORKER_NUM --partition_num=$PARTITION_NUM --granule_count=2000
                        --log_path=$LOG_PATH --lotus_checkpoint=$LOTUS_CHECKPOINT --persist_latency=0 --wal_group_commit_time=$WAL_GROUP_COMMIT_TIME --wal_group_commit_size=$WAL_GROUP_COMMIT_BATCH_SIZE
                        --partitioner=hash --hstore_command_logging=false
                        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 --time_to_run=$TIME_TO_RUN --time_to_warmup=$TIME_TO_WARMUP
                        --use_cxl_transport=$USE_CXL_TRANS --use_output_thread=$USE_OUTPUT_THREAD --cxl_trans_entry_struct_size=$CXL_TRANS_ENTRY_STRUCT_SIZE --cxl_trans_entry_num=$CXL_TRANS_ENTRY_NUM
                        --protocol=TwoPL --keys=$KEYS --zipf=$ZIPF_THETA --cross_ratio=$CROSS_RATIO" 0
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
if [ $# -lt 1 ]; then
        print_usage
        exit -1
fi

typeset RUN_TYPE=$1

# global configurations
typeset PASHA_CXL_TRANS_ENTRY_STRUCT_SIZE=2048
typeset PASHA_CXL_TRANS_ENTRY_NUM=8192

typeset BASELINE_CXL_TRANS_ENTRY_STRUCT_SIZE=65536
typeset BASELINE_CXL_TRANS_ENTRY_NUM=8192

if [ $RUN_TYPE = "TPCC" ]; then
        if [ $# != 18 ]; then
                print_usage
                exit -1
        fi

        typeset PROTOCOL=$2
        typeset HOST_NUM=$3
        typeset WORKER_NUM=$4
        typeset QUERY_TYPE=$5
        typeset REMOTE_NEWORDER_PERC=$6
        typeset REMOTE_PAYMENT_PERC=$7
        typeset USE_CXL_TRANS=$8
        typeset USE_OUTPUT_THREAD=$9
        typeset MIGRATION_POLICY=${10}
        typeset WHEN_TO_MOVE_OUT=${11}
        typeset HW_CC_BUDGET=${12}
        typeset SCC_MECH=${13}
        typeset PRE_MIGRATE=${14}
        typeset TIME_TO_RUN=${15}
        typeset TIME_TO_WARMUP=${16}
        typeset LOGGING_TYPE=${17}
        typeset GATHER_OUTPUT=${18}

        if [ $PROTOCOL = "SundialPasha" ] || [ $PROTOCOL = "TwoPLPasha" ]; then
                typeset CXL_TRANS_ENTRY_STRUCT_SIZE=$PASHA_CXL_TRANS_ENTRY_STRUCT_SIZE
                typeset CXL_TRANS_ENTRY_NUM=$PASHA_CXL_TRANS_ENTRY_NUM
        else
                typeset CXL_TRANS_ENTRY_STRUCT_SIZE=$BASELINE_CXL_TRANS_ENTRY_STRUCT_SIZE
                typeset CXL_TRANS_ENTRY_NUM=$BASELINE_CXL_TRANS_ENTRY_NUM
        fi

        typeset LOG_PATH="/root/pasha_log"
        if [ $LOGGING_TYPE = "WAL" ]; then
                typeset LOTUS_CHECKPOINT=1
                typeset WAL_GROUP_COMMIT_TIME=0
                typeset WAL_GROUP_COMMIT_BATCH_SIZE=0

        elif [ $LOGGING_TYPE = "GROUP_WAL" ]; then
                typeset LOTUS_CHECKPOINT=1
                typeset WAL_GROUP_COMMIT_TIME=40000     # SiloR uses 40ms
                typeset WAL_GROUP_COMMIT_BATCH_SIZE=10

        elif [ $LOGGING_TYPE = "BLACKHOLE" ]; then
                typeset LOTUS_CHECKPOINT=0
                typeset WAL_GROUP_COMMIT_TIME=0
                typeset WAL_GROUP_COMMIT_BATCH_SIZE=0
        else
                print_usage
                exit -1
        fi

        run_exp_tpcc $PROTOCOL $HOST_NUM $WORKER_NUM $QUERY_TYPE $REMOTE_NEWORDER_PERC $REMOTE_PAYMENT_PERC $USE_CXL_TRANS $USE_OUTPUT_THREAD $CXL_TRANS_ENTRY_STRUCT_SIZE $CXL_TRANS_ENTRY_NUM $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $HW_CC_BUDGET $SCC_MECH $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP $LOG_PATH $LOTUS_CHECKPOINT $WAL_GROUP_COMMIT_TIME $WAL_GROUP_COMMIT_BATCH_SIZE $GATHER_OUTPUT
        exit 0
elif [ $RUN_TYPE = "YCSB" ]; then
        if [ $# != 20 ]; then
                print_usage
                exit -1
        fi

        typeset PROTOCOL=$2
        typeset HOST_NUM=$3
        typeset WORKER_NUM=$4
        typeset QUERY_TYPE=$5
        typeset KEYS=$6
        typeset RW_RATIO=$7
        typeset ZIPF_THETA=$8
        typeset CROSS_RATIO=$9
        typeset USE_CXL_TRANS=${10}
        typeset USE_OUTPUT_THREAD=${11}
        typeset MIGRATION_POLICY=${12}
        typeset WHEN_TO_MOVE_OUT=${13}
        typeset HW_CC_BUDGET=${14}
        typeset SCC_MECH=${15}
        typeset PRE_MIGRATE=${16}
        typeset TIME_TO_RUN=${17}
        typeset TIME_TO_WARMUP=${18}
        typeset LOGGING_TYPE=${19}
        typeset GATHER_OUTPUT=${20}

        if [ $PROTOCOL = "SundialPasha" ] || [ $PROTOCOL = "TwoPLPasha" ]; then
                typeset CXL_TRANS_ENTRY_STRUCT_SIZE=$PASHA_CXL_TRANS_ENTRY_STRUCT_SIZE
                typeset CXL_TRANS_ENTRY_NUM=$PASHA_CXL_TRANS_ENTRY_NUM
        else
                typeset CXL_TRANS_ENTRY_STRUCT_SIZE=$BASELINE_CXL_TRANS_ENTRY_STRUCT_SIZE
                typeset CXL_TRANS_ENTRY_NUM=$BASELINE_CXL_TRANS_ENTRY_NUM
        fi

        typeset LOG_PATH="/root/pasha_log"
        if [ $LOGGING_TYPE = "WAL" ]; then
                typeset LOTUS_CHECKPOINT=1
                typeset WAL_GROUP_COMMIT_TIME=0
                typeset WAL_GROUP_COMMIT_BATCH_SIZE=0

        elif [ $LOGGING_TYPE = "GROUP_WAL" ]; then
                typeset LOTUS_CHECKPOINT=1
                typeset WAL_GROUP_COMMIT_TIME=100   # SiloR uses 40ms
                typeset WAL_GROUP_COMMIT_BATCH_SIZE=10

        elif [ $LOGGING_TYPE = "BLACKHOLE" ]; then
                typeset LOTUS_CHECKPOINT=0
                typeset WAL_GROUP_COMMIT_TIME=0
                typeset WAL_GROUP_COMMIT_BATCH_SIZE=0
        else
                print_usage
                exit -1
        fi

        run_exp_ycsb $PROTOCOL $HOST_NUM $WORKER_NUM $QUERY_TYPE $KEYS $RW_RATIO $ZIPF_THETA $CROSS_RATIO $USE_CXL_TRANS $USE_OUTPUT_THREAD $CXL_TRANS_ENTRY_STRUCT_SIZE $CXL_TRANS_ENTRY_NUM $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $HW_CC_BUDGET $SCC_MECH $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP $LOG_PATH $LOTUS_CHECKPOINT $WAL_GROUP_COMMIT_TIME $WAL_GROUP_COMMIT_BATCH_SIZE $GATHER_OUTPUT
        exit 0
elif [ $RUN_TYPE = "SmallBank" ]; then
        if [ $# != 18 ]; then
                print_usage
                exit -1
        fi

        typeset PROTOCOL=$2
        typeset HOST_NUM=$3
        typeset WORKER_NUM=$4
        typeset KEYS=$5
        typeset ZIPF_THETA=$6
        typeset CROSS_RATIO=$7
        typeset USE_CXL_TRANS=$8
        typeset USE_OUTPUT_THREAD=$9
        typeset MIGRATION_POLICY=${10}
        typeset WHEN_TO_MOVE_OUT=${11}
        typeset HW_CC_BUDGET=${12}
        typeset SCC_MECH=${13}
        typeset PRE_MIGRATE=${14}
        typeset TIME_TO_RUN=${15}
        typeset TIME_TO_WARMUP=${16}
        typeset LOGGING_TYPE=${17}
        typeset GATHER_OUTPUT=${18}

        if [ $PROTOCOL = "SundialPasha" ] || [ $PROTOCOL = "TwoPLPasha" ]; then
                typeset CXL_TRANS_ENTRY_STRUCT_SIZE=$PASHA_CXL_TRANS_ENTRY_STRUCT_SIZE
                typeset CXL_TRANS_ENTRY_NUM=$PASHA_CXL_TRANS_ENTRY_NUM
        else
                typeset CXL_TRANS_ENTRY_STRUCT_SIZE=$BASELINE_CXL_TRANS_ENTRY_STRUCT_SIZE
                typeset CXL_TRANS_ENTRY_NUM=$BASELINE_CXL_TRANS_ENTRY_NUM
        fi

        typeset LOG_PATH="/root/pasha_log"
        if [ $LOGGING_TYPE = "WAL" ]; then
                typeset LOTUS_CHECKPOINT=1
                typeset WAL_GROUP_COMMIT_TIME=0
                typeset WAL_GROUP_COMMIT_BATCH_SIZE=0

        elif [ $LOGGING_TYPE = "GROUP_WAL" ]; then
                typeset LOTUS_CHECKPOINT=1
                typeset WAL_GROUP_COMMIT_TIME=40000   # SiloR uses 40ms
                typeset WAL_GROUP_COMMIT_BATCH_SIZE=10

        elif [ $LOGGING_TYPE = "BLACKHOLE" ]; then
                typeset LOTUS_CHECKPOINT=0
                typeset WAL_GROUP_COMMIT_TIME=0
                typeset WAL_GROUP_COMMIT_BATCH_SIZE=0
        else
                print_usage
                exit -1
        fi

        run_exp_smallbank $PROTOCOL $HOST_NUM $WORKER_NUM $KEYS $ZIPF_THETA $CROSS_RATIO $USE_CXL_TRANS $USE_OUTPUT_THREAD $CXL_TRANS_ENTRY_STRUCT_SIZE $CXL_TRANS_ENTRY_NUM $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $HW_CC_BUDGET $SCC_MECH $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP $LOG_PATH $LOTUS_CHECKPOINT $WAL_GROUP_COMMIT_TIME $WAL_GROUP_COMMIT_BATCH_SIZE $GATHER_OUTPUT
        exit 0
elif [ $RUN_TYPE = "TATP" ]; then
        if [ $# != 18 ]; then
                print_usage
                exit -1
        fi

        typeset PROTOCOL=$2
        typeset HOST_NUM=$3
        typeset WORKER_NUM=$4
        typeset KEYS=$5
        typeset ZIPF_THETA=$6
        typeset CROSS_RATIO=$7
        typeset USE_CXL_TRANS=$8
        typeset USE_OUTPUT_THREAD=$9
        typeset MIGRATION_POLICY=${10}
        typeset WHEN_TO_MOVE_OUT=${11}
        typeset HW_CC_BUDGET=${12}
        typeset SCC_MECH=${13}
        typeset PRE_MIGRATE=${14}
        typeset TIME_TO_RUN=${15}
        typeset TIME_TO_WARMUP=${16}
        typeset LOGGING_TYPE=${17}
        typeset GATHER_OUTPUT=${18}

        if [ $PROTOCOL = "SundialPasha" ] || [ $PROTOCOL = "TwoPLPasha" ]; then
                typeset CXL_TRANS_ENTRY_STRUCT_SIZE=$PASHA_CXL_TRANS_ENTRY_STRUCT_SIZE
                typeset CXL_TRANS_ENTRY_NUM=$PASHA_CXL_TRANS_ENTRY_NUM
        else
                typeset CXL_TRANS_ENTRY_STRUCT_SIZE=$BASELINE_CXL_TRANS_ENTRY_STRUCT_SIZE
                typeset CXL_TRANS_ENTRY_NUM=$BASELINE_CXL_TRANS_ENTRY_NUM
        fi

        typeset LOG_PATH="/root/pasha_log"
        if [ $LOGGING_TYPE = "WAL" ]; then
                typeset LOTUS_CHECKPOINT=1
                typeset WAL_GROUP_COMMIT_TIME=0
                typeset WAL_GROUP_COMMIT_BATCH_SIZE=0

        elif [ $LOGGING_TYPE = "GROUP_WAL" ]; then
                typeset LOTUS_CHECKPOINT=1
                typeset WAL_GROUP_COMMIT_TIME=40000   # SiloR uses 40ms
                typeset WAL_GROUP_COMMIT_BATCH_SIZE=10

        elif [ $LOGGING_TYPE = "BLACKHOLE" ]; then
                typeset LOTUS_CHECKPOINT=0
                typeset WAL_GROUP_COMMIT_TIME=0
                typeset WAL_GROUP_COMMIT_BATCH_SIZE=0
        else
                print_usage
                exit -1
        fi

        run_exp_tatp $PROTOCOL $HOST_NUM $WORKER_NUM $KEYS $ZIPF_THETA $CROSS_RATIO $USE_CXL_TRANS $USE_OUTPUT_THREAD $CXL_TRANS_ENTRY_STRUCT_SIZE $CXL_TRANS_ENTRY_NUM $MIGRATION_POLICY $WHEN_TO_MOVE_OUT $HW_CC_BUDGET $SCC_MECH $PRE_MIGRATE $TIME_TO_RUN $TIME_TO_WARMUP $LOG_PATH $LOTUS_CHECKPOINT $WAL_GROUP_COMMIT_TIME $WAL_GROUP_COMMIT_BATCH_SIZE $GATHER_OUTPUT
        exit 0
elif [ $RUN_TYPE = "KILL" ]; then
        if [ $# != 2 ]; then
                print_usage
                exit -1
        fi

        typeset HOST_NUM=$2

        kill_prev_exps $HOST_NUM

        exit 0
elif [ $RUN_TYPE = "COMPILE" ]; then
        if [ $# != 1 ]; then
                print_usage
                exit -1
        fi

        # compile
        cd $SCRIPT_DIR/../
        mkdir -p build
        cd build
        cmake ..
        make -j

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
        if [ $# != 3 ]; then
                print_usage
                exit -1
        fi

        typeset HOST_NUM=$2
        typeset WORKER_NUM=$3

        echo "CI under construction!"

        exit -1
elif [ $RUN_TYPE = "COLLECT_OUTPUTS" ]; then
        if [ $# != 2 ]; then
                print_usage
                exit -1
        fi

        typeset HOST_NUM=$2

        gather_other_output $HOST_NUM

        exit 0
else
        print_usage
        exit -1
fi
