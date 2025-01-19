This repository contains source code for Pasha. The code is based on [lotus](https://github.com/DBOS-project/lotus).

# Setup Environment

```sh
# install gh
curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
&& sudo chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
&& echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
&& sudo apt update \
&& sudo apt install gh -y

# gh login
gh auth login

# clone repo
git clone https://github.com/pasha-project/pasha.git && cd pasha
git submodule update --init --recursive

# setup the current machine (the script will reboot the machine)
./scripts/setup.sh cur_host

# rebooting...
cd pasha

# build the image for our VMs after the reboot
./scripts/setup.sh vm_image

# setup dependencies
./scripts/setup.sh deps

# launch VMs
./scripts/setup.sh launch_vms chameleon 4 # we will launch 8 VMs each with 5 cores

# setup VMs
./scripts/setup.sh vms 4
```

# Build and Run Experiments

```sh
# build and sync
./scripts/run.sh COMPILE_SYNC 4

# run TPCC experiments (check run.sh for detailed usage)
./scripts/run.sh TPCC TwoPLPasha 4 1 mixed 0 0 1 0 1 Clock OnDemand 200000000 1 WriteThrough None 30 0 BLACKHOLE 20000 0 0

# run YCSB experiments (check run.sh for detailed usage)
./scripts/run.sh YCSB TwoPLPasha 4 1 rmw 100000 50 0 0 1 0 1 Clock OnDemand 200000000 1 WriteThrough NonPart 30 0 BLACKHOLE 20000 0 0
```
