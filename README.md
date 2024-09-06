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

# clone the repo
git clone https://github.com/pasha-project/pasha.git && cd pasha

# setup the current machine (the script will reboot the machine)
./scripts/setup.sh cur_host

# rebooting...
cd pasha

# build the image for our VMs after the reboot
./scripts/setup.sh vm_image

# setup dependencies
./scripts/setup.sh deps

# launch VMs
./scripts/setup.sh launch_vms 8 # we will launch 8 VMs each with 5 cores

# setup VMs
./scripts/setup.sh vms 8
```

# Build and Run Experiments

```sh
# build and sync
./scripts/run.sh COMPILE_SYNC 8 # we use 8 VMs

# run TPCC experiments (see run.sh for detailed usage)
./scripts/run.sh TPCC SundialPasha 8 2 10 15 1 4096 0

# run YCSB experiments (see run.sh for detailed usage)
./scripts/run.sh YCSB SundialPasha 8 2 50 0 10 1 4096 0
```
