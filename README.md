# Tigon
Tigon[^1](#1) is a research distributed transactional in-memory database that synchronizes cross-host concurrent data accesses over shared CXL memory. Tigon adopts the Pasha[^2](#2) architecture.
This repository is implemented based on the [lotus](https://github.com/DBOS-project/lotus) codebase from Xinjing Zhou.

This repository contains the following:
* An implementation of Tigon
* Sundial[^3](#3) optimized for a CXL pod: Sundial-CXL, Sundial+
* DS2PL[^4](#4) optimized for a CXL pod: DS2PL-CXL, and DS2PL+
* A benchmarking framework that supports full TPC-C, YCSB, SmallBank, and TATP (WIP)
* Scripts for emulating a CXL pod on a single physical machine
* Scripts for building and running Tigon
* Scripts for reproducing the results in the paper

## Claims
By running the experiments, you should be able to reproduce the numbers shown in:
* **Figure 5(a)**: TPC-C throughput of Sundial, Sundial-CXL, and Sundial+, varying percentages of multi-partition transactions.
* **Figure 5(b)**: TPC-C throughput of DS2PL, DS2PL-CXL, and DS2PL+, varying percentages of multi-partition transactions.
* **Figure 5(c)**: TPC-C throughput of Tigon, Sundial+, DS2PL+, and Motor, varying percentages of multi-partition transactions.
* **Figure 6**: YCSB throughput of Tigon, Sundial+, DS2PL+, and Motor, varying both read/write ratios and percentages of multi-partition transactions.
* **Figure 7**: Tigon's sensitivity to the size of hardware cache-coherent region.
* **Figure 8**: Comparison of different software cache-coherence protocols.

## Emulate a CXL Pod using VMs
We emulate a CXL pod by running multiple virtual machines (VMs) on a single host connected to a CXL 1.1 memory module. So each host running on a real CXL pod is emulated by a VM running on a single host sharing access to a CXL memory module.

![](emulation.png)

## Important Notes
* If you use one of our pre-configured machines, please skip [Testbed Setup](#Testbed-Setup) and jump to [Reproduce the Results with an All-in-one Script](#Reproduce-the-Results-with-an-All-in-one-Script) directly.
* We provide raw numbers for Motor (one of our baselines) in ``results/motor``. If you would like to run Motor, please refer to https://github.com/minghust/motor.
* Please execute the instructions in order and run all the commands in the project root directory.

## Testbed Setup

### Hardware Requirements

**Option A**
* A machine with at least 40 cores in one socket
* CXL memory connected to the first socket of the machine
* Ubuntu 22.04

**Option B** (if CXL memory is not available)
* A two-socket machine with at least 40 cores in each socket
* Ubuntu 22.04

### Setup VM-based CXL Pod Emulation from Scratch

1. Clone the repository
```bash
git clone https://github.com/yibo-huang/tigon.git
```

2. Install dependencies and switch the kernel to 5.19
```bash
./scripts/setup.sh DEPS # install dependencies
./scripts/setup.sh HOST # setup host and install kernel 5.19
sudo reboot # reboot to switch to the new kernel
```

3. Build VM image
```bash
./emulation/image/make_vm_img.sh
```

4. Launch VMs
```bash
# if you are using real CXL memory (Option A), run the following:
sudo daxctl reconfigure-device --mode=system-ram dax0.0 --force # manage CXL memory as a CPU-less NUMA node
sudo ./emulation/start_vms.sh --using-old-img --cxl 0 5 8 0 2 # replace the last argument with the NUMA node number of CXL memory (e.g., 2)

# if you emulate CXL memory using remote NUMA node (Option B), run the following:
sudo ./emulation/start_vms.sh --using-old-img --cxl 0 5 8 1 1 # launch 8 VMs each with 5 cores
```

5. Setup VMs
```bash
./scripts/setup.sh VMS 8 # 8 is the number of VMs
```

## Compile Tigon and Send the Binary to VMs
```bash
./scripts/run.sh COMPILE_SYNC 8 # 8 is the number of VMs
```

## Reproduce the Results with an All-in-one Script

We provide an all-in-one script for your convenience, which runs all the experiments and generates all the figures. The figures are stored in ``results/test1``. If you would like to run it multiple times, please use different directory names under ``results`` to avoid overwriting old results (e.g., ``results/test2``).
```bash
./scripts/run_all.sh results/test1 # use a different directory name under results each time to avoid overwriting old results
```

To inteprete the results:
* Figure 5 (a): ``results/tpcc/tpcc-sundial.pdf``
* Figure 5 (b): ``results/tpcc/tpcc-twopl.pdf``
* Figure 5 (c): ``results/tpcc/tpcc.pdf``
* Figure 6: ``results/ycsb/ycsb.pdf``
* Figure 7: ``results/hwcc_budget/hwcc_budget.pdf``
* Figure 8: ``results/swcc/swcc.pdf``

## Reproduce the Results One by One
### Reproduce Figure 5

```bash
./scripts/run_tpcc.sh ./results/test1 # run experiments
./scripts/parse/parse_tpcc.sh ./results/test1 # parse results
./scripts/plot/plot_tpcc_sundial.py ./results/test1 # generate Figure 5(a)
./scripts/plot/plot_tpcc_twopl.py ./results/test1 # generate Figure 5(b)
./scripts/plot/plot_tpcc.py ./results/test1 # generate Figure 5(c)
```
To inteprete the results:
* Figure 5 (a): ``results/tpcc/tpcc-sundial.pdf``
* Figure 5 (b): ``results/tpcc/tpcc-twopl.pdf``
* Figure 5 (c): ``results/tpcc/tpcc.pdf``

### Reproduce Figure 6

```bash
./scripts/run_ycsb.sh ./results/test1 # run experiments
./scripts/parse/parse_ycsb.py ./results/test1 # parse results
./scripts/plot/plot_ycsb.py ./results/test1 # generate Figure 6
```
The result pdf is ``results/ycsb/ycsb.pdf``

### Reproduce Figure 7

```bash
./scripts/run_hwcc_budget.sh ./results/test1 # run experiments
./scripts/parse/parse_hwcc_budget.py ./results/test1 # parse results
./scripts/plot/plot_hwcc_budget.py ./results/test1 # generate Figure 7
```
The result pdf is ``results/hwcc_budget/hwcc_budget.pdf``

### Reproduce Figure 8

```bash
./scripts/run_swcc.sh ./results/test1 # run experiments
./scripts/parse/parse_swcc.py ./results/test1 # parse results
./scripts/plot/plot_swcc.py ./results/test1 # generate Figure 8
```
The result pdf is ``results/swcc/swcc.pdf``

## Test Tigon in Various Configurations

Tigon is highly-configurable. We explain how to use ``scripts/run.sh`` to test Tigon in various configurations.

```bash
# run TPCC experiments
./scripts/run.sh TPCC CC_PROTOCOL HOST_NUM WORKER_NUM QUERY_TYPE REMOTE_NEWORDER_PERC REMOTE_PAYMENT_PERC USE_CXL_TRANS USE_OUTPUT_THREAD ENABLE_MIGRATION_OPTIMIZATION MIGRATION_POLICY WHEN_TO_MOVE_OUT HW_CC_BUDGET ENABLE_SCC SCC_MECH PRE_MIGRATE TIME_TO_RUN TIME_TO_WARMUP LOGGING_TYPE EPOCH_LEN MODEL_CXL_SEARCH GATHER_OUTPUTS

# example command to run TPC-C
./scripts/run.sh TPCC TwoPLPasha 8 3 mixed 0 0 1 0 1 Clock OnDemand 200000000 1 WriteThrough None 30 0 BLACKHOLE 20000 0 0

# run YCSB experiments
./scripts/run.sh YCSB CC_PROTOCOL HOST_NUM WORKER_NUM QUERY_TYPE KEYS RW_RATIO ZIPF_THETA CROSS_RATIO USE_CXL_TRANS USE_OUTPUT_THREAD ENABLE_MIGRATION_OPTIMIZATION MIGRATION_POLICY WHEN_TO_MOVE_OUT HW_CC_BUDGET ENABLE_SCC SCC_MECH PRE_MIGRATE TIME_TO_RUN TIME_TO_WARMUP LOGGING_TYPE EPOCH_LEN MODEL_CXL_SEARCH GATHER_OUTPUTS

# example command to run YCSB
./scripts/run.sh YCSB TwoPLPasha 8 3 rmw 100000 50 0 0 1 0 1 Clock OnDemand 200000000 1 WriteThrough NonPart 30 0 BLACKHOLE 20000 0 0
```
TPC-C Specific Arguments:
* ``QUERY_TYPE``: Query type to run. ``mixed`` includes all five transactions; ``first_two`` includes only the first two transactions
* ``REMOTE_NEWORDER_PERC``: Percentages of remote NewOrder transactions (0-100)
* ``REMOTE_PAYMENT_PERC``: Percentages of remote Payment transactions (0-100)

YCSB Specific Arguments:
* ``QUERY_TYPE``: Query type to run. ``rmw`` includes standard YCSB read/write queries; ``custom`` includes mixed inserts/deletes
* ``RW_RATIO``: Ratio of read/write operations. e.g., 50 means 50% read and 50% write
* ``ZIPF_THETA``: Skewness factor for Zipfian distribution (0-1)
* ``CROSS_RATIO``: Percentages of remote operations within a transaction (0-100)

Common Arguments:
* ``CC_PROTOCOL``: System to run. ``Sundial``, ``TwoPL``, ``TwoPLPasha`` (Tigon), ``TwoPLPashaPhantom`` (Tigon with phantom avoidance disabled).
* ``HOST_NUM``: Number of hosts (VMs)
* ``WORKER_NUM``: Number of transaction workers per host (VM)
* ``USE_CXL_TRANS``: Enable/disable CXL transport
* ``USE_OUTPUT_THREAD``: Enable/disable repurposing output threads for transaction processing. If enabled, ``USE_CXL_TRANS`` must also be enabled
* ``ENABLE_MIGRATION_OPTIMIZATION``: Enable/disable data movement optimization
* ``MIGRATION_POLICY``: Migration policy to use. ``Clock``, ``LRU``, ``FIFO``, ``NoMoveOut`` never moves out data.
* ``WHEN_TO_MOVE_OUT``: When to move out data. ``OnDemand`` triggers data moving out when CXL memory is full
* ``HW_CC_BUDGET``: The size of hardware cache-coherent CXL memory (MB)
* ``ENABLE_SCC``: Enable/disable software cache-coherence
* ``SCC_MECH``: Software cache-coherence protocol to use. ``WriteThrough`` is Tigon's default SWcc protocol; ``WriteThroughNoSharedRead`` disables shared reader; ``NonTemporal`` always do non-temporal access; ``NoOP`` always do temporal access
* ``PRE_MIGRATE``: Pre-migrate data before experiments. ``None`` migrates nothing; ``NonPart`` migrates non-partitionable data; ``All`` migrates all data
* ``TIME_TO_RUN``: Total run time for the experiment (s), including the warmup time
* ``TIME_TO_WARMUP``: Warmup time for the experiment (s)
* ``LOGGING_TYPE``: Logging mechanism to use. ``BLACKHOLE`` disables logging. ``GROUP_WAL`` enables epoch-based group commit
* ``EPOCH_LEN``: Epoch length (ms). Effective only when epoch-based group commit is enabled
* ``MODEL_CXL_SEARCH``: Enable/disable the shortcut pointer optimization
* ``GATHER_OUTPUTS``: Enable/disable collecting outputs from all hosts. If disabled, only host 1's output is shown

This script will print out statistics every second during your experiment, including transaction throughput, abort rate, data movement frequency, etc. After the experiment finishes, it will print out averaged statistics.

## References
<a id="1">[1]</a>
Tigon: A Distributed Database for a CXL Pod, *To appear at OSDI '25*\
<a id="2">[2]</a>
Pasha: An Efficient, Scalable Database Architecture for CXL Pods, *CIDR '25*\
<a id="3">[3]</a>
Sundial: Harmonizing Concurrency Control and Caching in a Distributed OLTP Database Management System, VLDB '18\
<a id="4">[4]</a>
Lotus: scalable multi-partition transactions on single-threaded partitioned databases, VLDB '22