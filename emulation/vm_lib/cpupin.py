from dataclasses import dataclass, field
from run_command import run_local_command

@dataclass(order=True)
class CpuLine:
    sort_index: int = field(init = False, repr = False)
    cpu: int
    core: int
    socket: int
    node: int

    def __post_init__(self):
        self.sort_index = self.core


def parse_lscpu_node(node_num: int = 0):
    ret = run_local_command(['lscpu', '-p'])
    cpus = ret.stdout.splitlines()
    node0_list = []
    for cpuline in cpus:
        if cpuline.startswith('#'):
            continue
        cpu, core, socket, node = map(int, cpuline.split(',')[:4])

        # On Chameleon@UC compute-cascadelake_r and Chameleon@TACC compute-zen3,
        # socket and node are equivalent. On Chameleon@UC storage-nvme, socket
        # is a NUMA domain from 0-1, and node is an inter-NUMA chiplet from 0-7
        # due to a BIOS setting. So for cross-platform compatibility, we use socket here.
        if socket == node_num:
            node0_list.append(CpuLine(cpu, core, socket, node))
    return sorted(node0_list)
