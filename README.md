This repository contains source code for Pasha. The code is based on [lotus](https://github.com/DBOS-project/lotus).

# Setup VM-based CXL Pod Emulation

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

# Build and Run Experiments

```sh
# build and sync
./scripts/run.sh COMPILE_SYNC 8

# run TPCC experiments (check run.sh for detailed usage)
./scripts/run.sh TPCC TwoPLPasha 8 3 mixed 0 0 1 0 1 Clock OnDemand 200000000 1 WriteThrough None 30 0 BLACKHOLE 20000 0 0

# run YCSB experiments (check run.sh for detailed usage)
./scripts/run.sh YCSB TwoPLPasha 8 3 rmw 100000 50 0 0 1 0 1 Clock OnDemand 200000000 1 WriteThrough NonPart 30 0 BLACKHOLE 20000 0 0
```

# Reproduce Figure 5

```sh
# run experiments
./scripts/run_tpcc.sh ./results/test1

# parse results
./scripts/parse/parse_tpcc.sh ./results/test1

# generate Figure 5 (a)
./scripts/plot/plot_tpcc_sundial.py ./results/test1

# generate Figure 5 (b)
./scripts/plot/plot_tpcc_twopl.py ./results/test1

# generate Figure 5 (c)
./scripts/plot/plot_tpcc.py ./results/test1
```

# Reproduce Figure 6

```sh
# run experiments
./scripts/run_ycsb.sh ./results/test1

# parse results
./scripts/parse/parse_ycsb.py ./results/test1

# generate Figure 6
./scripts/plot/plot_ycsb.py ./results/test1
