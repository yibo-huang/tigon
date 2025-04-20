import random
import re
import os
import json

from run_command import run_local_command, run_local_shell_command
from gpu import load_vfio_module, vfio_bind_dev


def get_rand_macaddr():
    rand1 = random.randint(0, 255)
    rand2 = random.randint(0, 255)
    rand_mac = f'DE:AD:BE:EF:{rand1:02X}:{rand2:02X}'
    return rand_mac


def get_rand_ib_macaddr():
    rand_mac = "DE:AD:BE:EF"
    for i in range(16):
        rand = random.randint(0, 255)
        rand_mac = rand_mac + f":{rand:02X}"
    return rand_mac


def find_mlnx_pf():
    ret = run_local_command(['sudo', 'lshw', '-c', 'network', '-businfo'])
    pci_devs = ret.stdout.splitlines()
    find_mlnx = re.compile('ConnectX', re.IGNORECASE)
    find_vf = re.compile('Virtual Function', re.IGNORECASE)
    mlnx_pf_ether_devs = []
    mlnx_pf_ib_devs = []
    for pci_dev in pci_devs:
        if find_mlnx.search(pci_dev) and not find_vf.search(pci_dev):
            pci_dev_arr = pci_dev.split()
            pci_dev = pci_dev_arr[0].split('@')[1]
            ether_dev = pci_dev_arr[1]
            ret = run_local_command(['ip', '-j', 'address', 'show', ether_dev])
            iface = json.loads(ret.stdout)[0]
            if iface['link_type'] == 'ether':
                # using the first one is just fine
                mlnx_pf_ether_devs.append(iface['ifname'])
                # used = False
                # for addr_info in iface['addr_info']:
                #     if addr_info['family'] == 'inet':
                #         used = True
                #         break
                # if not used:
                    # mlnx_pf_ether_devs.append(iface['ifname'])
            elif iface['link_type'] == 'infiniband':
                mlnx_pf_ib_devs.append((pci_dev, ether_dev))
    return mlnx_pf_ether_devs, mlnx_pf_ib_devs


def find_mlnx_vf(pf_dev):
    ret = run_local_command(['sudo', 'lshw', '-c', 'network', '-businfo'])
    pci_devs = ret.stdout.splitlines()
    find_mlnx = re.compile('ConnectX', re.IGNORECASE)
    find_vf = re.compile('Virtual Function', re.IGNORECASE)
    suffix = re.compile(r'v[0-9]*$')
    mlnx_vf_devs = []
    for pci_dev in pci_devs:
        if find_mlnx.search(pci_dev) and find_vf.search(pci_dev):
            pci_dev_arr = pci_dev.split()
            pci_dev = pci_dev_arr[0].split('@')[1]
            ether_dev = pci_dev_arr[1]
            ether_stem = suffix.split(ether_dev)[0]
            if ether_stem in pf_dev:
                mlnx_vf_devs.append((pci_dev, ether_dev))
    return mlnx_vf_devs


def create_mlnx_vf(pf_devs, num_vms: int, useIB: bool):
    if useIB:
        pf_ib_devs = pf_devs[1]
        if len(pf_ib_devs) == 0:
            raise RuntimeError("doesn't find ib devices")
        ib_dev = pf_ib_devs[0]
        print(f"Using {ib_dev} physical function to set up virtual functions")
        ret = run_local_shell_command(f"sudo cat /sys/class/infiniband/{ib_dev}/device/mlx5_num_vfs")
        num_vfs = int(ret.stdout.strip())
        if num_vfs == 0:
            run_local_shell_command(f"echo 16 | sudo tee /sys/class/infiniband/{ib_dev}/device/mlx5_num_vfs")
        elif num_vms > num_vfs:
            raise RuntimeError(f"vfs has set to {num_vfs} but num vms is {num_vms}.")
        return ib_dev
    else:
        pf_ether_devs = pf_devs[0]
        if len(pf_ether_devs) == 0:
            raise RuntimeError("doesn't find mlnx ether devices")
        ether_dev = pf_ether_devs[0]
        print(f"Using {ether_dev} physical function to set up virtual functions")
        ret = run_local_shell_command(f"sudo cat /sys/class/net/{ether_dev}/device/sriov_numvfs")
        num_vfs = int(ret.stdout.strip())
        if num_vfs == 0:
            run_local_shell_command(f"echo 16 | sudo tee /sys/class/net/{ether_dev}/device/sriov_numvfs")
        elif num_vms > num_vfs:
            raise RuntimeError(f"vfs has set to {num_vfs} but num vms is {num_vms}.")
        return ether_dev


def setup_mlnx_network(vmdir: str, num_vms: int, num_vfs_per_vm: int, useIB: bool):
    mlnx_pf_devs = find_mlnx_pf()
    pf_dev_vf_from = create_mlnx_vf(mlnx_pf_devs, num_vms, useIB)
    run_local_command(['sudo', 'ip', 'link', 'set', pf_dev_vf_from, 'up'])
    vf_devs = find_mlnx_vf(pf_dev_vf_from)
    ret = run_local_command(['sudo', 'iptables-save'])
    with open(os.path.join(vmdir, 'iptables.rules.old'), 'w') as f:
        f.write(ret.stdout)
    run_local_command(['sudo', 'sysctl', '-w', 'net.ipv4.ip_forward=1'])
    load_vfio_module()
    for i in range(num_vms * num_vfs_per_vm):
        run_local_command(['sudo', 'ip', 'link', 'set', pf_dev_vf_from, 'vf', str(i), 'state', 'enable'])
        vf_dev = vf_devs[i]
        pci_dev = vf_dev[0]
        print(f"unbind {pci_dev} from mlx5_core")
        run_local_shell_command(f'echo {pci_dev} | sudo tee /sys/bus/pci/drivers/mlx5_core/unbind', allow_fail=True)
        print(f"binding {pci_dev} to vfio-pci")
        vfio_bind_dev(pci_dev)
    return vf_devs
