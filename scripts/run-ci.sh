# TPCC
./bench_tpcc --logtostderr=1 --id=0 --servers="127.0.0.1:1234" --threads=2 --partition_num=2 --granule_count=2000 \
        --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0 \
        --partitioner=hash --hstore_command_logging=false \
        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 \
        --protocol=Sundial --query=mixed --neworder_dist=10 --payment_dist=15

./bench_tpcc --logtostderr=1 --id=0 --servers="127.0.0.1:1234" --threads=2 --partition_num=2 --granule_count=2000 \
        --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0 \
        --partitioner=hash --hstore_command_logging=false \
        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 \
        --protocol=HStore --query=mixed --neworder_dist=10 --payment_dist=15

# YCSB
./bench_ycsb --logtostderr=1 --id=0 --servers="127.0.0.1:1234" --threads=2 --partition_num=2 --granule_count=2000 \
        --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0 \
        --partitioner=hash --hstore_command_logging=false \
        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 \
        --protocol=Sundial --keys=100000 --read_write_ratio=100 --zipf=0 --cross_ratio=10 --cross_part_num=2

./bench_ycsb --logtostderr=1 --id=0 --servers="127.0.0.1:1234" --threads=2 --partition_num=2 --granule_count=2000 \
        --log_path= --persist_latency=0 --wal_group_commit_time=0 --wal_group_commit_size=0 \
        --partitioner=hash --hstore_command_logging=false \
        --replica_group=1 --lock_manager=0 --batch_flush=1 --lotus_async_repl=true --batch_size=0 \
        --protocol=HStore --keys=100000 --read_write_ratio=100 --zipf=0 --cross_ratio=10 --cross_part_num=2
