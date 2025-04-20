import subprocess as sp
from run_command import run_remote_shell_cmd, Fail_MSG



def get_ivshmem_bar2(vm_ip: str, vm_user: str = "root", vm_port: int = 22):
    ssh_str = f"-p {vm_port} {vm_user}@{vm_ip}"
    ret = run_remote_shell_cmd(ssh_str, 'lspci')
    for l in ret.stdout.splitlines():
        if "Inter-VM shared" in l:
            pci_addr = l.split()[0]
            ret2 = run_remote_shell_cmd(ssh_str, f'lspci -vvs {pci_addr}')
            for line in ret2.stdout.splitlines():
                if "Region 2" in line:
                    larr = line.split()
                    return int(larr[4])


def remove_ivshmem_bar2_mtrr(vm_ip: str, vm_user: str = "root", vm_port: int = 22):
    bar2_addr = get_ivshmem_bar2(vm_ip, vm_user, vm_port)
    ssh_str = f"-p {vm_port} {vm_user}@{vm_ip}"
    ret = run_remote_shell_cmd(ssh_str, "cat /proc/mtrr")
    for l in ret.stdout.splitlines():
        larr = l.split()
        base_addr = larr[1].split('=')[1]
        if base_addr == f'0x{bar2_addr}':
            reg = int(larr[0][3:-1])
            cmd = f"\"sudo bash -c 'echo -n disable={reg} > /proc/mtrr'\""
            ret = run_remote_shell_cmd(ssh_str, cmd)
            ret = run_remote_shell_cmd(ssh_str, "cat /proc/mtrr")
            print(ret.stdout)


# get_ivshmem_bar2("127.0.0.1", vm_port=10022)
# remove_ivshmem_bar2_mtrr("127.0.0.1", vm_port=10022)
