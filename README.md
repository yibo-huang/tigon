# Tigon: A Distributed Database for a CXL Pod
Building an efficient distributed transactional database remains a challenging problem despite decades of research. Existing distributed databases synchronize cross-host data accesses via networks which requires numerous network message exchanges and introduces inevitable overhead. We describe Tigon, the first distributed main-memory database that synchronizes cross-host concurrent data accesses via atomic operations on CXL memory, which is much more efficient than network-based approaches. However, Tigon must address the limitations of CXL memory: its higher latency and lower bandwidth relative to local DRAM, and its limited support for hardware cache coherence across hosts. Using TPC-C and a variant of YCSB, Tigon achieves up to 2.8x higher throughput compared to an optimized shared-nothing database that uses CXL memory as transport and up to 14.4x higher throughput compared to an RDMA-based distributed database.

## Setup VM-based CXL Pod Emulation

```sh
# clone repo
git clone https://github.com/yibo-huang/tigon.git && cd tigon

# install dependencies
./scripts/setup.sh DEPS

# setup current host and switch its kernel to 5.19
./scripts/setup.sh HOST

# please reboot to switch to the new kernel
sudo reboot

# after reboot
cd tigon

# build VM image
./emulation/image/make_vm_img.sh

# launch VMs
sudo ./emulation/start_vms.sh --using-old-img --cxl 0 5 8 1 1 # we will launch 8 VMs each with 5 cores

# setup VMs
./scripts/setup.sh VMS 8
```

## Build and Run Experiments

```sh
# build and sync
./scripts/run.sh COMPILE_SYNC 8

# run TPCC experiments (check run.sh for detailed usage)
./scripts/run.sh TPCC TwoPLPasha 8 3 mixed 0 0 1 0 1 Clock OnDemand 200000000 1 WriteThrough None 30 0 BLACKHOLE 20000 0 0

# run YCSB experiments (check run.sh for detailed usage)
./scripts/run.sh YCSB TwoPLPasha 8 3 rmw 100000 50 0 0 1 0 1 Clock OnDemand 200000000 1 WriteThrough NonPart 30 0 BLACKHOLE 20000 0 0
```

## Reproduce Figure 5

```sh
# run experiments
./scripts/run_tpcc.sh ./results/test1

# parse results
./scripts/parse/parse_tpcc.sh ./results/test1

# generate Figure 5(a)
./scripts/plot/plot_tpcc_sundial.py ./results/test1

# generate Figure 5(b)
./scripts/plot/plot_tpcc_twopl.py ./results/test1

# generate Figure 5(c)
./scripts/plot/plot_tpcc.py ./results/test1
```

## Reproduce Figure 6

```sh
# run experiments
./scripts/run_ycsb.sh ./results/test1

# parse results
./scripts/parse/parse_ycsb.py ./results/test1

# generate Figure 6
./scripts/plot/plot_ycsb.py ./results/test1
```

## Reproduce Figure 7

```sh
# run experiments
./scripts/run_hwcc_budget.sh ./results/test1

# parse results
./scripts/parse/parse_hwcc_budget.py ./results/test1

# generate Figure 7
./scripts/plot/plot_hwcc_budget.py ./results/test1
```

## Reproduce Figure 8

```sh
# run experiments
./scripts/run_swcc.sh ./results/test1

# parse results
./scripts/parse/parse_swcc.py ./results/test1

# generate Figure 8
./scripts/plot/plot_swcc.py ./results/test1
```

## Acknowledgement
This repo is implemented based on the [lotus](https://github.com/DBOS-project/lotus) repo from Xinjing Zhou.
