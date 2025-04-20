import argparse
import os
import re
import errno
import time
import shutil
import sys
from pathlib import Path
from typing import List, Optional
from dataclasses import dataclass, field
from pyroute2 import IPRoute
from pyroute2 import NetlinkError
from pwd import getpwnam
from pci_scanner import ScannerPCI
from run_command import run_local_command, run_local_command_parallel
from qmp import QEMUMonitorProtocol
from gpu import find_gpu, load_vfio_module, vfio_bind_dev, GPUKind, unload_amdgpu_module, unload_nvidia_module
from cpupin import parse_lscpu_node
from cpu import get_cpu_model, CPUKind
from network import get_rand_macaddr, setup_mlnx_network
from ivshmem import setup_cxl_host, start_ivshmem, cleanup_ivshmem_setup
from const import DEFAULT_DRIVE_PATH, DEFAULT_DRIVE_OVMF_PATH, DEFAULT_VM_DIR, LINUX_VM_BUILD_DIR
from mtrr import remove_ivshmem_bar2_mtrr

@dataclass
class VMConfig:
    qemu_bin: str = "/usr/bin/qemu-system-x86_64"
    kernel: Optional[str] = None
    initrd: Optional[str] = None
    drive: str = DEFAULT_DRIVE_PATH
    vmdir: str = DEFAULT_VM_DIR
    num_vms: int = 1
    num_cpus: int = 4
    mem_size_mb: int = 16 * 1024
    add_user_ssh: bool = False
    shmem_dir: Optional[str] = None
    shmem_size_mb: int = 4096
    shmem_dir_numa: list[int] = field(default_factory=lambda: [1])
    vm_numa_node: list[int] = field(default_factory=lambda: [0])
    msi_vectors: int = 8
    use_ivshmem_doorbell: bool = False
    mem_local_percent: int = -1
    pass_gpu: bool = False
    use_mlnx_ib: bool = False
    use_mlnx_ether: bool = False
    num_ether_per_vm: int = 1
    num_ib_per_vm: int = 0
    host_id: int = 0
    shared_dir: Optional[str] = None
    restart_vm: Optional[int] = None
    use_ovmf: bool = False


def setup_bridge_tap_network(vmdir: str, num_vms: int):
    bridge = 'br0'
    bridge_addr = '192.168.100.1'
    bridge_broadcast = '192.168.100.255'
    run_local_command(['sudo', 'modprobe', 'tun', 'tap'])
    ret = run_local_command(['sudo', 'iptables-save'])
    with open(os.path.join(vmdir, 'iptables.rules.old'), 'w') as f:
        f.write(ret.stdout)
    run_local_command(['sudo', 'sysctl', '-w', 'net.ipv4.ip_forward=1'])
    user_name = os.environ.get('USERNAME')
    if user_name is None:
        user_name = os.environ.get('USER')
    print(f"user name: {user_name}")
    uid = getpwnam(user_name).pw_uid

    with IPRoute() as ipr:
        br_arr = ipr.link_lookup(ifname=bridge)
        if len(br_arr) == 0:
            print(f"creating {bridge}")
            ipr.link("add", ifname=bridge,
                     kind="bridge")
        br = ipr.link_lookup(ifname=bridge)[0]
        ipr.link('set', index=br, state='down')
        try:
            ipr.addr('add', address=bridge_addr, mask=24,
                     broadcast=bridge_broadcast, index=br)
        except NetlinkError as e:
            if e.code == errno.EEXIST:
                pass
            else:
                raise
        print("creating tap device")
        for i in range(num_vms):
            tap_name = f'tap{i}'
            tap = ipr.link_lookup(ifname=tap_name)
            if len(tap) == 0:
                print(f"creating {tap_name}")
                ipr.link("add", ifname=tap_name,
                         kind="tuntap", mode="tap", uid=uid)
            tap = ipr.link_lookup(ifname=tap_name)[0]
            br = ipr.link_lookup(ifname=bridge)[0]
            ipr.link("set", index=tap, state='up')
            time.sleep(0.5)
            ipr.link("set", index=tap, master=br)
        ipr.link('set', index=br, state='up')


def sync_files_to_vm(vm_img: str, vm_mnt_dir: str, src_dir: str, dest_dir: str):
    if os.path.exists(src_dir):
        run_local_command(["sudo", "guestmount", "-a", vm_img, "-i", vm_mnt_dir], capture=False)
        run_local_command(["sudo", "mkdir", "-p", dest_dir])
        excludes = ['--exclude', '*.h', '--exclude', '*.cmake', '--exclude', '*.c', '--exclude', 'man', '--exclude', '*.a',
                    '--exclude', 'tests', '--exclude', 'test', '--exclude', '__pycache__']
        run_local_command(['sudo', 'rsync', '-av', *excludes,
                           src_dir, dest_dir], capture=False)
        run_local_command(["sudo", "umount", vm_mnt_dir], capture=False)
        run_local_command(["sync"], capture=False)


def construct_basic_qemu_cmd(args: argparse.Namespace, vm_id: int):
    qemu_monitor_path = os.path.join(args.vmdir, f'{vm_id}/qemu-monitor.sock')
    cpukind = get_cpu_model()
    cpu_model = "host"
    if cpukind == CPUKind.AMD_EPYC:
        cpu_model = "EPYC,topoext"

    trace = []
    if os.path.exists("/tmp/events"):
       trace = ["--trace", "events=/tmp/events"]

    numa_cpu = ",".join(map(str, args.vm_numa_node))
    gdb_port = 1234 + vm_id
    qemu_cmd = ["numactl", f"--cpunodebind={numa_cpu}", "--", f"{args.qemu_bin}", *trace,
        "-machine", "q35,accel=kvm,mem-merge=off", "-cpu", cpu_model, "-D", f"./qemu_log{vm_id}.txt",
        "-m", f"{args.mem_size_mb}M,maxmem={args.mem_size_mb}M", "--overcommit", "mem-lock=on",
        "--overcommit", "cpu-pm=on",
        "-smp", f"{args.num_cpus},maxcpus={args.num_cpus},sockets=1,cores={args.num_cpus}",
        "-enable-kvm", "-display", "none", "-daemonize",
        "-device", "virtio-rng-pci",
        "-qmp", f"unix:{qemu_monitor_path},server,nowait",
        "-gdb", f"tcp::{gdb_port}", # for gdb debug
        "-pidfile", f"{args.vmdir}/{vm_id}/pid",
    ]
    if args.use_ovmf:
        qemu_cmd += [
            "-drive", f"if=pflash,format=raw,file={args.vmdir}/OVMF_CODE.fd,readonly=on",
            "-drive", f"if=pflash,format=raw,file={args.vmdir}/{vm_id}/OVMF_VARS.fd"]

    if args.kernel and args.initrd:
        qemu_cmd += ["-kernel", args.kernel, "-initrd", args.initrd,
                     "-append", "selinux=0 audit=0 console=ttyS0 root=/dev/vda2 ignore_loglevel rw"]
    return qemu_cmd


def construct_blk_cmd(qemu_cmd: List[str], numcpus: int, vmdrive_file: str):
    # qemu_cmd += ["-object", "iothread,id=iothread0",
    #     "-device", f"virtio-blk-pci,packed=on,num-queues={numcpus},iothread=iothread0,drive=drive0,id=virblk0",
    #     "-drive", f"if=none,file={vmdrive_file},format=raw,media=disk,id=drive0"]
    qemu_cmd += [
        "-device", f"virtio-blk-pci,packed=on,num-queues=1,drive=drive0,id=virblk0",
        "-drive", f"if=none,file={vmdrive_file},format=raw,media=disk,id=drive0,cache=none,aio=native"]


def construct_tap_net_cmd(qemu_cmd: List[str], vm_id: int):
    rand_mac = get_rand_macaddr()
    qemu_cmd += ["-device", f"virtio-net-pci,mq=on,packed=on,netdev=network{vm_id},mac={rand_mac},romfile=",
        "-netdev", f"tap,id=network{vm_id},vhost=on,ifname=tap{vm_id},script=no,downscript=no"]


def construct_sriov_vfio_net_cmd(qemu_cmd: List[str], vm_id: int, vf_devs: List, num_vfs_per_vm: int):
    # assign virtual functions in sequence
    for i in range(num_vfs_per_vm):
        vf_dev = vf_devs[vm_id * num_vfs_per_vm + i]
        qemu_cmd += ["-device", f"vfio-pci,host={vf_dev[0]}",
                 "-nic", "none"]


def construct_user_net_cmd(qemu_cmd: List[str], vm_id: int):
    ssh_port = 10022 + vm_id
    qemu_cmd += ["-device", f"virtio-net-pci,netdev=netssh{vm_id}",
        "-netdev", f"user,id=netssh{vm_id},hostfwd=tcp::{ssh_port}-:22"]


def setup_shared_mem(vmdir: str, num_vms: int, shmem_dir: str, shmem_dir_numa: list[int], shmem_size_mb: int,
                     msi_vectors: int, use_ivshmem_doorbell: bool):
    cxl_dir_size = shmem_size_mb + 512
    setup_cxl_host(shmem_dir, cxl_dir_size, shmem_dir_numa)
    if use_ivshmem_doorbell:
        ivshmem_sock = start_ivshmem(vmdir, num_vms, shmem_dir, shmem_size_mb, msi_vectors)
        return ivshmem_sock
    else:
        shmem_path = os.path.join(shmem_dir, "mem_1")
        run_local_command(["truncate", "-s", f"{shmem_size_mb}M", shmem_path])
        return shmem_path


# by default, vms are in node 0 and shared mem is on node 1 for two socket setup
# when using doorbell, the shmem_path is the ivshmem-server's unix socket
# when using plain, the shmem_path is the file path of the backing file
def construct_shared_mem(qemu_cmd: List[str], vm_id: int, shmem_path: str, shmem_size_mb: int,
                         msi_vectors: int, use_ivshmem_doorbell: bool):
    # https://www.qemu.org/docs/master/system/devices/ivshmem.html
    if use_ivshmem_doorbell:
        qemu_cmd += [
            "-device",
            f"ivshmem-doorbell,vectors={msi_vectors},chardev=ivshmem-server",
            "-chardev",
            f"socket,path={shmem_path + '.' + str(vm_id + 1)},id=ivshmem-server"
        ]
    else:
        qemu_cmd += [
            "-device",
            "ivshmem-plain,memdev=ivshmem",
            "-object",
            f"memory-backend-file,size={shmem_size_mb}M,share=on,mem-path={shmem_path},id=ivshmem"
        ]


# https://wiki.qemu.org/Documentation/9psetup
# https://superuser.com/questions/628169/how-to-share-a-directory-with-the-host-without-networking-in-qemu
def construct_shared_dir(qemu_cmd: List[str], shared_dir: str):
    qemu_cmd += [
        "-virtfs",
        f"local,path={shared_dir},mount_tag=host,security_model=none",
    ]


def construct_vnode_single(qemu_cmd: List[str], mem_size_mb: int):
    qemu_cmd += ["-object", f"memory-backend-ram,id=mem0,size={mem_size_mb}M,prealloc=on,host-nodes=0,policy=bind,merge=off",
        "-numa", "node,nodeid=0,memdev=mem0",
        "-numa", "cpu,node-id=0,socket-id=0",
        "-numa", "dist,src=0,dst=0,val=10"]


# vnodeid: 0 or 1
# hnodeid: host/backing node ID to allocate VM memory from
# mem_size_mb: mem size in MB
# vcpus: vcpus for this vnode, e.g. "0-7" (optional -> computeless-vnode)
def construct_vnode(qemu_cmd: List[str], numcpus: int, vnodeid: int,
                              hnodeid: int, mem_size_mb: int, vcpus: str):
    ram_backend = f"memory-backend-ram,size=${mem_size_mb}M,policy=bind,host-nodes={hnodeid}," \
                  f"id=ram-node{vnodeid},prealloc=on,prealloc-threads={numcpus}"
    cmd=["-object", ram_backend]

    numa_node_config = "node,nodeid=${vnodeid},"
    if vcpus != "":
        numa_node_config += f"cpus={vcpus},"
    numa_node_config += f"memdev=ram-node{vnodeid}"
    cmd += ["-numa", numa_node_config]
    qemu_cmd += cmd


def create_static_ip_addr_network_config(per_vmdir: str, ip_addr: str, network_card_id: int):
    network_template_path = os.path.join(LINUX_VM_BUILD_DIR, "20-wired-template.network")
    with open(network_template_path, "r") as f:
        network_template = f.read()
    wired_20 = network_template.replace("@ADDR@", ip_addr)
    wired_20 = wired_20.replace("@COUNT@", str(4+network_card_id))
    wired_20_file = os.path.join(per_vmdir, f"{2+network_card_id}0-wired.network")
    with open(wired_20_file, "w") as f:
        f.write(wired_20)
    return wired_20_file


def create_dhcp_network_config_for_userssh(per_vmdir: str, network_cards: int):
    network_template_path = os.path.join(LINUX_VM_BUILD_DIR, "30-wired-template.network")
    with open(network_template_path, "r") as f:
        network_template = f.read()
    wired_30 = network_template.replace("@COUNT@", str(4+network_cards))
    wired_30_file = os.path.join(per_vmdir, f"{2+network_cards}0-wired.network")
    with open(wired_30_file, "w") as f:
        f.write(wired_30)
    return wired_30_file


def create_static_ip_addr_ib_config(per_vmdir: str, ip_addr: str):
    network_template_path = os.path.join(LINUX_VM_BUILD_DIR, "40-wired-template.network")
    with open(network_template_path, "r") as f:
        network_template = f.read()
    wired_40 = network_template.replace("@ADDR@", ip_addr)
    wired_40_file = os.path.join(per_vmdir, "40-wired.network")
    with open(wired_40_file, "w") as f:
        f.write(wired_40)
    return wired_40_file


def create_etc_hosts(per_vmdir: str, ip_addr: str):
    etchosts_template_path = os.path.join(LINUX_VM_BUILD_DIR, "etc_hosts_template")
    with open(etchosts_template_path, "r") as f:
        etchosts_template = f.read()
    etchosts = etchosts_template.replace("@ADDR@", ip_addr)
    etchosts_file = os.path.join(per_vmdir, "hosts")
    with open(etchosts_file, "w") as f:
        f.write(etchosts)
    return etchosts_file


def copy_vm_disk(top_vmdir: str, num_vms: int, drive: str, drive_file_name: str):
    copy_cmds = []
    for vm_id in range(num_vms):
        per_vmdir = os.path.join(top_vmdir, str(vm_id))
        os.makedirs(per_vmdir, exist_ok=True)
        per_vm_drive = os.path.join(per_vmdir, drive_file_name)
        if not os.path.exists(per_vm_drive):
            cmd = ['dd', f'if={drive}', f'of={per_vm_drive}',
                   'iflag=direct', 'oflag=direct', 'bs=64K', 'conv=sparse']
            copy_cmds.append(cmd)
    run_local_command_parallel(copy_cmds)


def prepare_vmdir(top_vmdir: str, drive: str, vm_id: int, drive_file_name: str,
                  ether_card_per_vm: int = 1, ib_card_per_vm: int = 0, user_ssh: bool = False,
                  num_vms: int = 1, host_id: int = 0):
    per_vmdir = os.path.join(top_vmdir, str(vm_id))
    os.makedirs(per_vmdir, exist_ok=True)
    network_dir = os.path.join(per_vmdir, 'mnt', 'etc', 'systemd', 'network')
    etc_dir = os.path.join(per_vmdir, 'mnt', 'etc')
    mount_dir = os.path.join(per_vmdir, 'mnt')
    os.makedirs(mount_dir, exist_ok=True)
    vmdrive_file = os.path.join(per_vmdir, drive_file_name)

    shutil.copy('/usr/share/OVMF/OVMF_VARS.fd', per_vmdir)
    per_vm_drive = os.path.join(per_vmdir, drive_file_name)
    if not os.path.exists(per_vm_drive):
        run_local_command(['dd', f'if={drive}', f'of={per_vm_drive}',
            'iflag=direct', 'oflag=direct', 'bs=64K', 'conv=sparse'])

    # ensuring different VMs on different hosts do not have overlapped IP addresses
    num_ip_required = num_vms * (ether_card_per_vm + ib_card_per_vm)
    ip = 2 + (vm_id * ether_card_per_vm + vm_id * ib_card_per_vm) + host_id * num_ip_required
    network_files = []
    for i in range(ether_card_per_vm):
        ip_addr = f'192.168.100.{ip}'
        ip += 1
        wired_20_file = create_static_ip_addr_network_config(per_vmdir, ip_addr, i)
        network_files.append(wired_20_file)
    for i in range(ib_card_per_vm):
        ip_addr = f'192.168.100.{ip}'
        ip += 1
        wired_40_file = create_static_ip_addr_ib_config(per_vmdir, ip_addr)
        network_files.append(wired_40_file)
    if user_ssh:
        dhcp_file = create_dhcp_network_config_for_userssh(per_vmdir, ether_card_per_vm + ib_card_per_vm)
        network_files.append(dhcp_file)
    etchosts_file = create_etc_hosts(per_vmdir, ip_addr)

    etcgai_path = os.path.join(LINUX_VM_BUILD_DIR, "gai.conf")

    run_local_command(["sudo", "guestmount", "-a",
        vmdrive_file, "-i", mount_dir])
    for f in network_files:
        run_local_command(["sudo", "mv", f, network_dir])
    run_local_command(["sudo", "mv", etchosts_file, etc_dir])
    run_local_command(["sudo", "cp", etcgai_path, etc_dir])
    # run_local_command(["sudo", "cp", os.path.join(LINUX_VM_BUILD_DIR, '30-wired.network'), network_dir])
    run_local_command(["sudo", "umount", mount_dir])
    run_local_command(["sync"])
    time.sleep(3)


def pin_vm_threads(args: argparse.Namespace, vms: list[int]):
    cpulist = []
    for node in args.vm_numa_node:
        cpulist += parse_lscpu_node(node)
    cpulist_len = len(cpulist)
    for vm_id in vms:
        qemu_monitor_path = os.path.join(args.vmdir, f'{vm_id}/qemu-monitor.sock')
        srv = QEMUMonitorProtocol(qemu_monitor_path)
        srv.connect()
        num_cpus = int(args.num_cpus)
        for vcpu in srv.command('query-cpus-fast'):
            tid = vcpu['thread-id']
            cpuidx = vcpu['cpu-index'] + vm_id * num_cpus
            cpu_to_pin = cpulist[cpuidx % cpulist_len].cpu
            print(f'Pin vCPU {cpuidx} of vm{vm_id}(tid {tid}) to physical CPU {cpu_to_pin}')
            run_local_command(['taskset', '-pc', str(cpu_to_pin), str(tid)])
        # for iothread in srv.command('query-iothreads'):
        #     tid = iothread['thread-id']
        #     idx = num_vms * num_cpus + vm_id
        #     cpu_to_pin = cpulist[idx % cpulist_len].cpu
        #     print(f'Pin iothread of vm{vm_id}(tid {tid}) to physical CPU {cpu_to_pin}')
        #     run_local_command(['taskset', '-pc', str(cpu_to_pin), str(tid)])


def unset_driver_overrides():
    ret = run_local_command(['driverctl', 'list-overrides'], allow_fail=True)
    overrides = []
    for l in ret.stdout.splitlines():
        overrides.append(l.split()[0])
    find_mlnx = re.compile('ConnectX', re.IGNORECASE)
    for pci_dev in overrides:
        run_local_command(['sudo', 'driverctl', 'unset-override', pci_dev])
        ret = run_local_command(['lspci', '-s', pci_dev])
        if find_mlnx.search(ret.stdout.strip()):
            run_local_command(['sudo', 'driverctl', '--nosave', 'set-override', pci_dev, 'mlx5_core'])


def start_vms(args: argparse.Namespace):
    print(f"start vm with config: {args}")
    if args.drive == DEFAULT_DRIVE_PATH and args.use_ovmf:
        args.drive = DEFAULT_DRIVE_OVMF_PATH

    # One-time global setup
    if args.restart_vm is None:
        if run_local_command(["pgrep", "qemu-system"], allow_fail=True).returncode == 0:
            print("WARNING: found existing qemu-system processes, will interfere with start_vm", file=sys.stderr)
            sys.exit(1)

        os.makedirs(args.vmdir, exist_ok=True)
        if args.pass_gpu or args.use_mlnx_ib or args.use_mlnx_ether:
            unset_driver_overrides()
        vf_devs = []
        num_ether_per_vm = 1
        num_ib_per_vm = 0
        if args.use_mlnx_ib:
            vf_devs = setup_mlnx_network(args.vmdir, int(args.num_vms), int(args.num_ib_per_vm), True)
            num_ib_per_vm = args.num_ib_per_vm
        elif args.use_mlnx_ether:
            vf_devs = setup_mlnx_network(args.vmdir, int(args.num_vms), int(args.num_ether_per_vm), False)
            num_ether_per_vm = args.num_ether_per_vm
        else:
            setup_bridge_tap_network(args.vmdir, int(args.num_vms))
        shutil.copy('/usr/share/OVMF/OVMF_CODE.fd', args.vmdir)
        drive_file_name = os.path.basename(args.drive)
        num_vms = int(args.num_vms)
        if args.pass_gpu:
            pci_devs, kind = find_gpu()
        else:
            pci_devs, kind = [], GPUKind.NONE
        if args.shmem_dir:
            shmem_path = setup_shared_mem(args.vmdir, num_vms, args.shmem_dir, args.shmem_dir_numa,
                            int(args.shmem_size_mb), args.msi_vectors,
                            args.use_ivshmem_doorbell)

    vms = list(range(num_vms)) if args.restart_vm is None else [args.restart_vm]

    # Per-VM setup
    for vm_id in vms:
        if args.restart_vm is None:
            copy_vm_disk(args.vmdir, num_vms, args.drive, drive_file_name)
            prepare_vmdir(args.vmdir, args.drive, vm_id, drive_file_name,
                        num_ether_per_vm, num_ib_per_vm,
                        args.add_user_ssh, args.num_vms, args.host_id)

        vmdrive_file = os.path.join(args.vmdir, str(vm_id), drive_file_name)
        qemu_cmd = construct_basic_qemu_cmd(args, vm_id)
        construct_blk_cmd(qemu_cmd, args.num_cpus, vmdrive_file)
        if args.use_mlnx_ib:
            construct_sriov_vfio_net_cmd(qemu_cmd, vm_id, vf_devs, args.num_ib_per_vm)
        elif args.use_mlnx_ether:
            construct_sriov_vfio_net_cmd(qemu_cmd, vm_id, vf_devs, args.num_ether_per_vm)
        else:
            construct_tap_net_cmd(qemu_cmd, vm_id)
        if args.add_user_ssh:
            construct_user_net_cmd(qemu_cmd, vm_id)
        if args.shmem_dir:
            construct_shared_mem(qemu_cmd, vm_id, shmem_path, int(args.shmem_size_mb), args.msi_vectors,
                                 args.use_ivshmem_doorbell)
        if args.shared_dir:
            construct_shared_dir(qemu_cmd, args.shared_dir)
        if args.mem_local_percent == 100:
            construct_vnode(qemu_cmd, args.num_cpus, 0, 0, args.mem_size_mb, f"0-{args.num_cpus-1}")
        elif args.mem_local_percent == 0:
            construct_vnode(qemu_cmd, args.num_cpus, 0, 1, args.mem_size_mb, "")
        elif args.mem_local_percent != -1:
            vnode0 = int(args.mem_size_mb * args.mem_local_percent / 100.0)
            vnode1 = args.mem_size_mb - vnode0
            construct_vnode(qemu_cmd, args.num_cpus, 0, 0, vnode0, f"0-{args.num_cpus-1}")
            construct_vnode(qemu_cmd, args.num_cpus, 1, 1, vnode1, "")
        else:
            construct_vnode_single(qemu_cmd, args.mem_size_mb)
        if args.pass_gpu:
            pass_gpu(qemu_cmd, vm_id, pci_devs, kind)
        run_local_command(qemu_cmd, capture=False)

    time.sleep(5)
    pin_vm_threads(args, vms)
    print("Remember to run python3 rm_bar2mtrr --ip <ip> --port <port> --username <username>")


def pass_gpu(qemu_cmd: List[str], vm_id: int, pci_devs: List[str], kind: GPUKind):
    if pci_devs and (kind == GPUKind.NVIDIA or kind == GPUKind.AMD):
        if len(pci_devs) <= vm_id:
            raise ValueError(f"not enough GPUs({len(pci_devs)}) to pass to all vm{vm_id}")
        if kind == GPUKind.NVIDIA:
            print("stop nvidia persistent daemon...")
            run_local_command(['sudo', 'systemctl', 'stop', 'nvidia-persistenced.service'], allow_fail=True)
            print("unload nvidia module")
            unload_nvidia_module()
        if kind == GPUKind.AMD:
            print("unload amdgpu module...")
            unload_amdgpu_module()
        print("loading vfio driver...")
        load_vfio_module()
        pci_dev = pci_devs[vm_id]
        print(f"bind pci dev {pci_dev} to vfio...")
        vfio_bind_dev(pci_dev)
        qemu_cmd += ['-device', f'vfio-pci,host={pci_dev}']


def get_ivshmem_pci(args: argparse.Namespace):
    scanner = ScannerPCI(ip=args.ip, username=args.username)
    for device in scanner.select(name="*Inter-VM*"):
        print(device.pci_address)


def cleanup_ivshmem(args: argparse.Namespace):
    cleanup_ivshmem_setup(args.shmem_dir)


def rm_bar2_mtrr(args: argparse.Namespace):
    remove_ivshmem_bar2_mtrr(args.ip, args.username, args.port)


def main():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers()

    parser_start_vm = subparsers.add_parser('start_vm', help='start vm')
    parser_start_vm.add_argument('--qemu_bin', default="qemu-system-x86_64", help="qemu binary to use")
    parser_start_vm.add_argument('--kernel', help="path to the kernel file")
    parser_start_vm.add_argument('--initrd', help="path to the initrd file")
    parser_start_vm.add_argument('--drive', default=DEFAULT_DRIVE_PATH, help="path to the VM drive")
    parser_start_vm.add_argument('--num_cpus', type=int, default=4, help="number of vCPUs")
    parser_start_vm.add_argument('--mem_size_mb', type=int, default=4096, help="memory size in MiB")
    parser_start_vm.add_argument('--vmdir', default=DEFAULT_VM_DIR, help="top level directory for VMs")
    parser_start_vm.add_argument('--num_vms', type=int, default=1, help="number of VMs to start")
    parser_start_vm.add_argument('--add_user_ssh', action='store_true',
                        help="whether includes the user space network for ssh forwarding")
    parser_start_vm.add_argument('--shmem_dir', type=str, help="shared memory dir")
    parser_start_vm.add_argument('--shmem_dir_numa', type=int, nargs="+", default = [1], help="Which socket the shared memory dir should be in; default to 1")
    parser_start_vm.add_argument('--shmem_size_mb', type=int, default=4096, help='shared memory size in MiB')
    parser_start_vm.add_argument('--vm_numa_node', type=int, nargs="+", default=[0], help='Which socket should the vm runs on; default to 0')
    parser_start_vm.add_argument('--shared_dir', type=str, help='host directory to map into VMs')
    parser_start_vm.add_argument('--msi_vectors', type=int, default=8, help='ivpci msi vectors')
    parser_start_vm.add_argument('--use-ivshmem-doorbell', action='store_true',
                                 help="whether to use ivshmem-doorbell")
    parser_start_vm.add_argument('--mem_local_percent', default=-1,
                        help="-1 disables CXL emulation, 0-100 enables CXL emulation " \
                             "with the specified percentage of memory allocated to the local node")
    parser_start_vm.add_argument('--pass_gpu', action='store_true',
                        help="whether passthrough GPU to VM")
    parser_start_vm.add_argument('--use_mlnx_ib', action='store_true',
                        help="whether to use mlnx infiniband sriov virtual function")
    parser_start_vm.add_argument('--use_mlnx_ether', action='store_true',
                        help="whether to use mlnx ethernet sriov virtual function")
    parser_start_vm.add_argument('--num_ether_per_vm', default=1,
                                 help="number of ethernet virtual function assigned to a vm", type=int)
    parser_start_vm.add_argument('--num_ib_per_vm', default=0,
                                 help="number of infiniteband virtual function assigned to a vm", type=int)
    parser_start_vm.add_argument('--host_id', default=0,
                                 help="ID of the host machine", type=int)
    parser_start_vm.add_argument('--restart_vm', type=int, help="restart a crashed VM in this group")
    parser_start_vm.add_argument('--use_ovmf', type=bool, default=False, help="whether using OVMF or not")
    parser_start_vm.set_defaults(func=start_vms)

    parser_rm_mtrr = subparsers.add_parser('rm_bar2mtrr', help='remove bar2 mtrr')
    parser_rm_mtrr.add_argument('--ip', type=str, required=True, help='vm ip addr')
    parser_rm_mtrr.add_argument('--port', type=int, default=22, help='ssh port')
    parser_rm_mtrr.add_argument('--username', type=str, default="root")
    parser_rm_mtrr.set_defaults(func=rm_bar2_mtrr)

    parser_ivshmem_pci = subparsers.add_parser('get_ivshmem_pci', help='get ivshmem pci address')
    parser_ivshmem_pci.add_argument('--ip', type=str, required=True, help="vm ip addr")
    parser_ivshmem_pci.add_argument('--port', type=int, default=22, help='ssh port')
    parser_ivshmem_pci.add_argument('--username', type=str, default="root")
    parser_ivshmem_pci.add_argument('--filename', type=str, default=str(Path.home() / ".ssh"/ "id_rsa"))
    parser_ivshmem_pci.set_defaults(func=get_ivshmem_pci)

    parser_cleanup_ivshmem = subparsers.add_parser('clean_ivshmem', help='cleanup ivshmem host memory and kill daemon')
    parser_cleanup_ivshmem.add_argument('--shmem_dir', type=str, required=True, help="shared memory dir")
    parser_cleanup_ivshmem.set_defaults(func=cleanup_ivshmem)

    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    main()
