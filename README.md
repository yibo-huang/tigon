# Tigon
Tigon is a research distributed in-memory OLTP database that synchronizes cross-host concurrent data accesses over shared CXL memory.

This repository contains the following:
* An implementation of Tigon.
* Baselines optimized for a CXL pod: Sundial-CXL, Sundial+, DS2PL-CXL, and DS2PL+.
* A benchmarking framework that supports full TPC-C, YCSB, and SmallBank.
* Scripts for emulating a CXL pod on a single physical machine.
* Scripts for building and running Tigon.
* Scripts for reproducing the results in the paper.

## Claims
By running the experiments, you should be able to reproduce the numbers shown in:
* **Figure 5(a)**: TPC-C throughput of Sundial, Sundial-CXL, and Sundial+, varying percentages of multi-partition transactions.
* **Figure 5(b)**: TPC-C throughput of DS2PL, DS2PL-CXL, and DS2PL+, varying percentages of multi-partition transactions.
* **Figure 5(c)**: TPC-C throughput of Tigon, Sundial+, DS2PL+, and Motor, varying percentages of multi-partition transactions.
* **Figure 6**: YCSB throughput of Tigon, Sundial+, DS2PL+, and Motor, varying both read/write ratios and percentages of multi-partition transactions.
* **Figure 7**: Tigon's sensitivity to the size of hardware cache-coherent region.
* **Figure 8**: Comparison of different software cache-coherence protocols.

## Emulate a CXL Pod using VMs
As shown in the figure below, we emulate a CXL pod by running multiple VMs on a single host connected to a CXL 1.1 memory module.

![](emulation.png)

## Important Notes
* We are unable to provide access to real CXL memory hardware due to its limited availability. Instead, we provide pre-configured two-socket machines to emulate it using remote NUMA memory. Please check HotCRP for details.
* If you decide to use one of our pre-configured machines, please skip [Testbed Setup](#Testbed-Setup) and jump to [Compile and Run Tigon](#Compile-and-Run-Tigon) directly.
* **Although the numbers obtained using emulated CXL memory may not be exactly the same as the paper, the overall trends should be the same.**
* We provide raw numbers for Motor (one of our baselines) in ``results/motor``.
* Please execute the instructions in order and run all the commands at the project root directory.

## Testbed Setup

1. Clone the repo
```bash
git clone https://github.com/yibo-huang/tigon.git
```

2. Install dependencies and switch the kernel to 5.19
```bash
./scripts/setup.sh DEPS # install dependencies
./scripts/setup.sh HOST # setup current host and install the 5.19 kernel
sudo reboot # reboot to switch to the new kernel
```

3. Build VM image
```bash
./emulation/image/make_vm_img.sh # build VM image
```

4. Launch VMs
```bash
# if you emulate CXL memory using remote NUMA node, run the following:
sudo ./emulation/start_vms.sh --using-old-img --cxl 0 5 8 1 1 # launch 8 VMs each with 5 cores


# if you use real CXL memory, run the following:
sudo daxctl reconfigure-device --mode=system-ram dax0.0 --force # manage CXL memory as a CPU-less NUMA node
sudo ./emulation/start_vms.sh --using-old-img --cxl 0 5 8 0 2 # replace the last argument with the node number of CXL memory
```

5. Setup VMs
```bash
./scripts/setup.sh VMS 8 # 8 is the number of VMs
```

## Compile Tigon and Send it to VMs
```bash
./scripts/run.sh COMPILE_SYNC 8 # 8 is the number of VMs
```

## Reproduce the Results

We provide an all-in-one script for your conn
```bash
./scripts/run_all.sh
```

## Detailed instructions

This section will guide you on how to configure, build, and run all the experiments **from scratch**.
If you have access to a pre-configured cluster, skip to [building and deploying the binaries](#Building-and-Deploying-the-Binaries).

## Build and Run Experiments

```bash
# build and sync
./scripts/run.sh COMPILE_SYNC 8

# run TPCC experiments (check run.sh for detailed usage)
./scripts/run.sh TPCC TwoPLPasha 8 3 mixed 0 0 1 0 1 Clock OnDemand 200000000 1 WriteThrough None 30 0 BLACKHOLE 20000 0 0

# run YCSB experiments (check run.sh for detailed usage)
./scripts/run.sh YCSB TwoPLPasha 8 3 rmw 100000 50 0 0 1 0 1 Clock OnDemand 200000000 1 WriteThrough NonPart 30 0 BLACKHOLE 20000 0 0
```

### Reproduce Figure 5

```bash
./scripts/run_tpcc.sh ./results/test1 # run experiments
./scripts/parse/parse_tpcc.sh ./results/test1 # parse results
./scripts/plot/plot_tpcc_sundial.py ./results/test1 # generate Figure 5(a)
./scripts/plot/plot_tpcc_twopl.py ./results/test1 # generate Figure 5(b)
./scripts/plot/plot_tpcc.py ./results/test1 # generate Figure 5(c)
```

### Reproduce Figure 6

```bash
./scripts/run_ycsb.sh ./results/test1 # run experiments
./scripts/parse/parse_ycsb.py ./results/test1 # parse results
./scripts/plot/plot_ycsb.py ./results/test1 # generate Figure 6
```

### Reproduce Figure 7

```bash
./scripts/run_hwcc_budget.sh ./results/test1 # run experiments
./scripts/parse/parse_hwcc_budget.py ./results/test1 # parse results
./scripts/plot/plot_hwcc_budget.py ./results/test1 # generate Figure 7
```

### Reproduce Figure 8

```bash
./scripts/run_swcc.sh ./results/test1 # run experiments
./scripts/parse/parse_swcc.py ./results/test1 # parse results
./scripts/plot/plot_swcc.py ./results/test1 # generate Figure 8
```

## Acknowledgement
This repo is implemented based on the [lotus](https://github.com/DBOS-project/lotus) repo from Xinjing Zhou.
