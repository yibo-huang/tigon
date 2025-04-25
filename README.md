# Tigon
Tigon is a research distributed transactional in-memory database that synchronizes cross-host concurrent data accesses over shared CXL memory.

This repository contains the following:
* An implementation of Tigon.
* Baselines optimized for a CXL pod: Sundial-CXL, Sundial+, DS2PL-CXL, and DS2PL+.
* A benchmarking framework that supports full TPC-C, YCSB, SmallBank, and TATP (WIP).
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
We emulate a CXL pod by running multiple virtual machines (VMs) on a single host connected to a CXL 1.1 memory module. So each host running on a real CXL pod is emulated by a VM running on a single host sharing access to a CXL memory module.

![](emulation.png)

## Important Notes
* If you use one of our pre-configured machines, please skip [Testbed Setup](#Testbed-Setup) and jump to [Reproduce the Results with an All-in-one Script](#Reproduce-the-Results-with-an-All-in-one-Script) directly.
* We provide raw numbers for Motor (one of our baselines) in ``results/motor``. If you would like to run Motor, please refer to https://github.com/minghust/motor.
* Please execute the instructions in order and run all the commands in the project root directory.

## Testbed Setup

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
# if you emulate CXL memory using remote NUMA node, run the following:
sudo ./emulation/start_vms.sh --using-old-img --cxl 0 5 8 1 1 # launch 8 VMs each with 5 cores


# if you are using real CXL memory, run the following:
sudo daxctl reconfigure-device --mode=system-ram dax0.0 --force # manage CXL memory as a CPU-less NUMA node
sudo ./emulation/start_vms.sh --using-old-img --cxl 0 5 8 0 2 # replace the last argument with the NUMA node number of CXL memory (e.g., 2)
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

We provide an all-in-one script for your convenience, which runs all the experiments and generates all the figures. The figures are stored in ``results/test1``. If you would like to run it multiple times to obtain more results, please use different directory names under ``results`` to avoid overwriting old results (e.g., ``results/test2``).
```bash
./scripts/run_all.sh results/test1 # use a different directory name under results each time to avoid overwriting old results
```

## Reproduce the Results One by One
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

## Play Around

Tigon is highly tunable. Here we provide instructions to run and test Tigon with different configurations.

```bash
# run TPCC experiments (check run.sh for detailed usage)
./scripts/run.sh TPCC TwoPLPasha 8 3 mixed 0 0 1 0 1 Clock OnDemand 200000000 1 WriteThrough None 30 0 BLACKHOLE 20000 0 0

# run YCSB experiments (check run.sh for detailed usage)
./scripts/run.sh YCSB TwoPLPasha 8 3 rmw 100000 50 0 0 1 0 1 Clock OnDemand 200000000 1 WriteThrough NonPart 30 0 BLACKHOLE 20000 0 0
```

## Acknowledgement
This repository is implemented based on [lotus](https://github.com/DBOS-project/lotus) from Xinjing Zhou.
