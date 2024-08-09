**Xinjing Zhou**, Xiangyao Yu, Goetz Graefe, Micheal Stonebraker

[Lotus: Scalable Multi-Partition Transactions on Single-Threaded Partitioned Databases](https://doi.org/10.14778/3551793.3551843)

*Proc. of the VLDB Endowment (PVLDB), Volume 15, Sydney, Australia, 2022.*

This repository contains source code for Lotus. The code is based on the [star](https://github.com/luyi0619/star) framework from Yi Lu.

# Dependencies

```sh
sudo apt-get update
sudo apt-get install -y zip make cmake g++ libjemalloc-dev libboost-dev libgoogle-glog-dev
```

# Download

```sh
git clone https://github.com/DBOS-project/lotus.git
```

# Build

```
./compile.sh
```

# Reproducing Experiments

Note that the tutorial only works for Google Cloud Compute Engine.

Make sure you have placed the source code folder under `~`.

Make sure every node in the cluster has installed all the software dependencies.

Make sure the benchmark has been compiled on every node using `compile.sh`.

We assume the log files for transactions are placed under `/mnt/disks/nvme/`.

The sample scripts provided run on 6 nodes.

## Figure 9(a): Comparison with Non-Deterministic Systems

1. Fill in `scripts/ips.txt` with ip addresses of the nodes you want to run the experiments on.
2. Fill in `scripts/instance_names.txt` with corresponding instance names (Google Cloud Compute Engine instance name) of the nodes supplied in `scripts/ips.txt`.  
4. Run the following bash code on the first node of the cluster to distribute benchmarking scripts to other nodes.
```bash
cd scripts
#                           port to run the experiments on
#                             |
python distribute_script.py 1234 gc_2pl_mp_ycsb.py  us-central1-a # ----- Google Cloud region name
#                                     |
#                                 baseline-specific distribution script
python distribute_script.py 1234 gc_sundial_mp_ycsb.py us-central1-a
python distribute_script.py 1234 gc_hstore_mp_ycsb.py us-central1-a
python distribute_script.py 1234 gc_lotus_mp_ycsb.py us-central1-a
```
5. Run the following code on the first node of the cluster to start the benchmark
```bash
sh run_2pl_mp_ycsb.sh
sh run_sundial_mp_ycsb.sh
sh run_hstore_mp_ycsb.sh
sh run_lotus_mp_ycsb_sync.sh
```
6. Results are placed under `~/exp_results` on the first node of the cluster.

## Figure 10(a): Comparison with Deterministic Systems

1. Fill in `scripts/ips.txt` with ip addresses of the nodes you want to run the experiments on.
2. Fill in `scripts/instance_names.txt` with corresponding instance names (Google Cloud Compute Engine instance name) of the nodes supplied in `scripts/ips.txt`.  
3. Fill in `scripts/ips_half.txt` with first half of the nodes from `scripts/ips.txt`. This is for Calvin and Aria baselines that only need to evaluate the performance of one replica (3 nodes).
4. Fill in `scripts/instance_names_half.txt` with first half of the nodes from `scripts/instance_names.txt`. 
5. Run the following bash code on the first node of the cluster to distribute benchmarking scripts to other nodes.
```bash
cd scripts
python distribute_script_half.py 1234 gc_aria_mp_ycsb.py us-central1-a
python distribute_script_half.py 1234 gc_calvin_mp_ycsb.py us-central1-a
python distribute_script.py 1234 gc_lotus_mp_ycsb.py us-central1-a
```
6. Run the following code on the first node of the cluster to start the benchmark
```bash
sh run_aria_mp_ycsb.sh
sh run_calvin_mp_ycsb.sh
sh run_lotus_mp_ycsb.sh
```
7. Results are placed under `~/exp_results` on the first node of the cluster.
