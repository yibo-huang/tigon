#! /bin/bash

set -x
set -euo pipefail

typeset SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
typeset BUILD_DIR=${SCRIPT_DIR}

function print_usage {
        echo "[usage] ./make_vm_img.sh"
}

if [ $# != 0 ]; then
        print_usage
        exit -1
fi

typeset mkosi_bin="python3 -m mkosi"
typeset mkosi_opts=("-f")

enable_systemd_service()
{
	servicename="$1"

	if [ ! -d "mkosi.extra" ]; then
		fail "couldn't find mkosi.extra, are we in an unexpected CWD?"
	fi

	mkdir -p mkosi.extra/etc/systemd/system/multi-user.target.wants

	ln -sf "/usr/lib/systemd/system/${servicename}.service" \
		"mkosi.extra/etc/systemd/system/multi-user.target.wants/${servicename}.service"
}

mkdir -p ${BUILD_DIR}/mkosi.extra
mkdir -p ${BUILD_DIR}/mkosi.cache ${BUILD_DIR}/mkosi.builddir

cd ${BUILD_DIR}

# misc rootfs setup
mkdir -p mkosi.extra/root/.ssh
cp -L ~/.ssh/id_rsa.pub mkosi.extra/root/.ssh/authorized_keys
cp ~/.ssh/id_rsa.pub mkosi.extra/root/.ssh
cp ~/.ssh/id_rsa mkosi.extra/root/.ssh
touch ${BUILD_DIR}/mkosi.extra/root/.ssh/config
chmod +w ${BUILD_DIR}/mkosi.extra/root/.ssh/config || true
echo -e "Host *\n    StrictHostKeyChecking no" >> ${BUILD_DIR}/mkosi.extra/root/.ssh/config
chmod 400 ${BUILD_DIR}/mkosi.extra/root/.ssh/config
chmod -R go-rwx mkosi.extra/root

source $SCRIPT_DIR/ubuntu_rootfs.sh mkosi.extra
cp -Lr ~/.bash* ${BUILD_DIR}/mkosi.extra/root/

if [ -f /etc/localtime ]; then
	mkdir -p mkosi.extra/etc/
	sudo cp -P /etc/localtime mkosi.extra/etc/
fi

mkdir -p ${BUILD_DIR}/mkosi.extra/etc/modules-load.d/
mkdir -p ${BUILD_DIR}/mkosi.extra/etc/chrony/
echo ptp_kvm > ${BUILD_DIR}/mkosi.extra/etc/modules-load.d/ptp_kvm.conf
echo "refclock PHC /dev/ptp0 poll 2" >> ${BUILD_DIR}/mkosi.extra/etc/chrony/chrony.conf

echo "deb http://us.archive.ubuntu.com/ubuntu/ jammy main restricted universe multiverse" > ${BUILD_DIR}/mkosi.extra/etc/apt/sources.list
echo "deb http://us.archive.ubuntu.com/ubuntu/ jammy-updates main restricted universe multiverse" >> ${BUILD_DIR}/mkosi.extra/etc/apt/sources.list
echo "deb http://us.archive.ubuntu.com/ubuntu/ jammy-backports main restricted universe multiverse" >> ${BUILD_DIR}/mkosi.extra/etc/apt/sources.list
echo "deb http://security.ubuntu.com/ubuntu jammy-security main restricted universe multiverse" >> ${BUILD_DIR}/mkosi.extra/etc/apt/sources.list

# network_device=$(lspci -Dmnn | grep '\[02')
# if echo ${network_device} | grep -i mellanox >/dev/null 2>/dev/null; then
#     gh release download bin_release --skip-existing -p "*.tgz"
#     mv ./MLNX_OFED_LINUX-23.04-0.5.3.3-ubuntu22.04-x86_64.tgz ${BUILD_DIR}/mkosi.extra/root/
# fi

if echo ${network_device} | grep -i "Ethernet Controller E810-C" >/dev/null 2>/dev/null; then
    wget https://sourceforge.net/projects/e1000/files/iavf%20stable/4.8.2/iavf-4.8.2.tar.gz -O ${BUILD_DIR}/mkosi.extra/root/iavf-4.8.2.tar.gz
fi

enable_systemd_service sshd

enable_systemd_service systemd-networkd
enable_systemd_service systemd-resolved
enable_systemd_service chrony

mkdir -p mkosi.extra/etc/sysctl.d/
tee mkosi.extra/etc/sysctl.d/99-kubernetes-cri.conf <<EOF
net.bridge.bridge-nf-call-iptables  = 1
net.ipv4.ip_forward                 = 1
net.bridge.bridge-nf-call-ip6tables = 1
EOF

mkosi_ver="$($mkosi_bin --version | awk '/mkosi/{ print $2 }')"
if (( mkosi_ver >= 9 )); then
	mkosi_opts+=("--autologin")
fi
mkosi_opts+=("build")

sudo -E env "PATH=$PATH" $mkosi_bin ${mkosi_opts[@]}
sudo chmod go+rw root.img
cd -
