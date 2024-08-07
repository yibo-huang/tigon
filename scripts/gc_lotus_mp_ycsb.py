import sys

ips = [line.strip() for line in open("ips.txt", "r")]
n = len(ips)

ins = [line.split(" ")[0] for line in ips]
outs = [line.split(" ")[1] for line in ips]

id = int(sys.argv[1]) 
port = int(sys.argv[2]) 

ratios = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
#ratios = [40, 60, 70]
#ratios = [0, 5, 10, 25, 50, 75, 100]
def get_cmd(n, i):
  cmd = ""
  for j in range(n):
    if j > 0:
      cmd += ";"
    if id == j:
      cmd += ins[j] + ":" + str(port+i)
    else:
      cmd += outs[j] + ":" + str(port+i)
  return cmd

for i,ratio in enumerate(ratios):
  cmd = get_cmd(n, i)
  print('./bench_ycsb --logtostderr=1 --id=%d --servers="%s" --threads=6 --read_write_ratio=50 --partition_num=$partition_num --keys=100000 --granule_count=$granule_count --log_path=/mnt/disks/nvme/coord0 --persist_latency=$logging_latency --wal_group_commit_time=1000 --wal_group_commit_size=0 --partitioner=hpb --hstore_command_logging=true --protocol=HStore --replica_group=3 --lock_manager=1 --batch_size=$bsize --batch_flush=1 --cross_ratio=%d --lotus_async_repl=$lotus_async_repl --cross_part_num=$cross_part_num' % (id, cmd, ratio) )
  print('sleep 10s')
