#!/bin/bash
set -xeuo pipefail

sudo apt-get -y update
sudo apt-get -y upgrade
# sudo apt-get -y install --install-recommends linux-generic-hwe-22.04

# 5.19.0-50
sudo apt-get -y install --install-recommends linux-image-5.19.0-50-generic

# main line linux
# mkdir -p /etc/apt/keyrings/
# curl -fsSL https://pkgs.zabbly.com/key.asc -o /etc/apt/keyrings/zabbly.asc
# 
# sh -c 'cat <<EOF > /etc/apt/sources.list.d/zabbly-kernel-stable.sources
# Enabled: yes
# Types: deb
# URIs: https://pkgs.zabbly.com/kernel/stable
# Suites: $(. /etc/os-release && echo ${VERSION_CODENAME})
# Components: main
# Architectures: $(dpkg --print-architecture)
# Signed-By: /etc/apt/keyrings/zabbly.asc
# 
# EOF'

# sudo apt-get -y update
# apt-get install -y linux-image-6.9.9-zabbly+

echo "deb http://ddebs.ubuntu.com $(lsb_release -cs) main restricted universe multiverse
deb http://ddebs.ubuntu.com $(lsb_release -cs)-updates main restricted universe multiverse
deb http://ddebs.ubuntu.com $(lsb_release -cs)-proposed main restricted universe multiverse" | \
sudo tee -a /etc/apt/sources.list.d/ddebs.list > /dev/null
sudo apt install -y ubuntu-dbgsym-keyring

# azul java
sudo apt install -y gnupg ca-certificates curl

curl -s https://repos.azul.com/azul-repo.key | sudo gpg --dearmor -o /usr/share/keyrings/azul.gpg

echo "deb [signed-by=/usr/share/keyrings/azul.gpg] https://repos.azul.com/zulu/deb stable main" | sudo tee /etc/apt/sources.list.d/zulu.list > /dev/null

sudo apt-get -y update || true
kernel_image=$(sudo apt list --installed | egrep -o linux-image-[0-9].[0-9]*.0-[0-9]*-generic | tail -n1)
kernel_version=$(echo $kernel_image | sed 's/linux-image-//')
sudo apt install -y linux-tools-${kernel_version} linux-cloud-tools-${kernel_version} linux-modules-extra-${kernel_version} zulu8-jdk
sudo update-grub
wget https://apt.llvm.org/llvm.sh -O llvm.sh && chmod +x llvm.sh && sudo ./llvm.sh 18 clang-18 lldb-18 lld-18 \
    clang-tools-18 lld-18 lldb-18 llvm-18-tools

curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
        && sudo chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
        && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | sudo tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
        && sudo apt update \
        && sudo apt install -y gh

video_device=$(sudo lshw -C video)
if echo ${video_device} | grep -i nvidia >/dev/null 2>/dev/null; then
    if echo ${video_device} | grep -i "Tesla K40m"; then
        sudo apt-get install -y nvidia-utils-470-server nvidia-headless-470-server nvidia-kernel-common-470-server nvidia-modprobe
    else
        sudo apt-get install -y nvidia-headless-530 nvidia-utils-530 nvidia-kernel-common-530 nvidia-modprobe
    fi
elif echo ${video_device} | grep -i amd >/dev/null 2>/dev/null; then
    wget https://repo.radeon.com/amdgpu-install/5.4.2/ubuntu/jammy/amdgpu-install_5.4.50402-1_all.deb
    sudo apt-get install -y ./amdgpu-install_5.4.50402-1_all.deb
    sudo amdgpu-install -y --no-32  --accept-eula --usecase=dkms
    sudo apt-get -y install rocm-smi-lib
    rm amdgpu-install_5.4.50402-1_all.deb
fi

network_device=$(lspci -Dmnn | grep '\[02')
if echo ${network_device} | grep -i mellanox >/dev/null 2>/dev/null; then
    # cd /root
    # tar -xf ./MLNX_OFED_LINUX-23.04-0.5.3.3-ubuntu22.04-x86_64.tgz
    # cd ./MLNX_OFED_LINUX-23.04-0.5.3.3-ubuntu22.04-x86_64
    # sudo ./mlnxofedinstall --without-dkms --add-kernel-support --without-fw-update --force
    # cd ..
    # rm ./MLNX_OFED_LINUX-23.04-0.5.3.3-ubuntu22.04-x86_64.tgz
    # rm -rf ./MLNX_OFED_LINUX-23.04-0.5.3.3-ubuntu22.04-x86_64
    export DOCA_URL="https://linux.mellanox.com/public/repo/doca/2.7.0/ubuntu22.04/x86_64/"
    curl https://linux.mellanox.com/public/repo/doca/GPG-KEY-Mellanox.pub | gpg --dearmor | sudo tee /etc/apt/trusted.gpg.d/GPG-KEY-Mellanox.pub > /dev/null
    echo "deb [signed-by=/etc/apt/trusted.gpg.d/GPG-KEY-Mellanox.pub] $DOCA_URL ./" | sudo tee /etc/apt/sources.list.d/doca.list > /dev/null
    sudo apt-get update
    sudo apt-get -y install doca-ofed
    echo "ib_ipoib" >> /etc/modules
fi

if echo ${network_device} | grep -i "Ethernet Controller E810-C" >/dev/null 2>/dev/null; then
    cd /root
    tar -xvf iavf-4.8.2.tar.gz
    cd ./iavf-4.8.2/src
    sudo make install
    cd ../..
    rm iavf-4.8.2.tar.gz
    rm -rf iavf-4.8.2
fi

wget https://github.com/photoszzt/mem_workloads/releases/download/v0.1-alpha-model/pcm-0000-Linux.deb
sudo apt-get install ./pcm-0000-Linux.deb
rm ./pcm-0000-Linux.deb
python3 -m pip install ruamel.yaml
