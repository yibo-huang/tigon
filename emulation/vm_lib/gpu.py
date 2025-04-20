from enum import Enum
import re
from run_command import run_local_command


class GPUKind(Enum):
    NVIDIA = 1
    AMD = 2
    NONE = 3


def find_gpu():
    ret = run_local_command(['lspci', '-Dmnn'])
    pci_devs = ret.stdout.splitlines()
    find_display = re.compile('\[03', re.IGNORECASE)
    find_nvidia = re.compile('nvidia', re.IGNORECASE)
    find_amd = re.compile('amd', re.IGNORECASE)
    gpu_devs = []
    kind = GPUKind.NONE
    for pci_dev in pci_devs:
        if find_display.search(pci_dev):
            if find_nvidia.search(pci_dev):
                if kind == GPUKind.NONE:
                    kind = GPUKind.NVIDIA
                pci_dev = pci_dev.split()[0]
                gpu_devs.append(pci_dev)
            elif find_amd.search(pci_dev):
                if kind == GPUKind.NONE:
                    kind = GPUKind.AMD
                pci_dev = pci_dev.split()[0]
                gpu_devs.append(pci_dev)
    return gpu_devs, kind


def unload_nvidia_module():
    run_local_command(['sudo', 'modprobe', '-r', 'nvidia_uvm'], allow_fail=True)
    run_local_command(['sudo', 'modprobe', '-r', 'nvidia_drm'], allow_fail=True)
    run_local_command(['sudo', 'modprobe', '-r', 'nvidia_modeset'], allow_fail=True)
    run_local_command(['sudo', 'modprobe', '-r', 'nvidia'], allow_fail=True)


def unload_amdgpu_module():
    run_local_command(['sudo', 'modprobe', '-r', 'amdgpu'])


def load_nvidia_module():
    run_local_command(['sudo', 'modprobe', 'nvidia_uvm'])
    run_local_command(['sudo', 'modprobe', 'nvidia_drm'])
    run_local_command(['sudo', 'modprobe', 'nvidia_modeset'])
    run_local_command(['sudo', 'modprobe', 'drm_kms_helper'])
    run_local_command(['sudo', 'modprobe', 'drm'])
    run_local_command(['sudo', 'modprobe', 'nvidia'])


def load_vfio_module():
    run_local_command(['sudo', 'modprobe', 'vfio'])
    run_local_command(['sudo', 'modprobe', 'vfio_pci'])
    run_local_command(['sudo', 'modprobe', 'vfio_iommu_type1'])


def unload_vfio_module():
    run_local_command(['sudo', 'modprobe', '-r', 'vfio'])
    run_local_command(['sudo', 'modprobe', '-r', 'vfio_pci'])
    run_local_command(['sudo', 'modprobe', '-r', 'vfio_iommu_type1'])


def vfio_bind_dev(pci_dev: str):
    run_local_command(['sudo', 'driverctl', '--nosave', 'set-override', pci_dev, 'vfio-pci'])

