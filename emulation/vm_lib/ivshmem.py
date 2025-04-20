import os
import sys
import time
import random
import string
import subprocess as sp
from const import SCRIPT_DIR
from run_command import run_local_command, run_local_command_allow_fail, run_local_shell_command


def setup_cxl_host(cxl_host_dir: str, cxl_host_dir_size_mb: int, shmem_dir_numa: list[int]):
    run_local_command(f"sudo mkdir -p {cxl_host_dir}".split())
    numa = ",".join(map(str, shmem_dir_numa))
    if os.path.ismount(cxl_host_dir):
        run_local_command(f"sudo umount {cxl_host_dir}".split())
    if not os.path.ismount(cxl_host_dir):
        run_local_command(f"sudo mount -t tmpfs -o size={cxl_host_dir_size_mb}M "
                          f"-o mpol=bind:{numa} -o rw,nosuid,nodev tmpfs {cxl_host_dir}".split())


def cleanup_ivshmem_setup(cxl_host_dir: str):
    run_local_command_allow_fail(f"pkill -9 qemu".split())
    run_local_command_allow_fail(f"pkill -9 ivshmem".split())
    time.sleep(3)
    if os.path.ismount(cxl_host_dir):
        run_local_command(f"sudo umount {cxl_host_dir}".split())


def start_ivshmem(vmdir: str,
                  num_vms: int,
                  shmem_dir: str,
                  shmem_size_mb: int,
                  msi_vectors: int):

    rust_ivshmem = os.path.join(SCRIPT_DIR, "../ivshmem/ivshmem-host")
    rust_ivshmem_target = os.path.join(rust_ivshmem,  "target/release/")
    ivshmem_server_path = os.path.join(rust_ivshmem_target, "ivshmem-server")

    if not os.path.exists(ivshmem_server_path):
        run_local_shell_command(f"cd {rust_ivshmem} && cargo build --release")

    ivshmem_server_sock = os.path.join(vmdir, "ivshmem_sock")
    cxl_path = os.path.join(shmem_dir, "mem_1")
    if not os.path.exists(cxl_path):
        with open(cxl_path, "a") as out:
            out.truncate(shmem_size_mb * 1024)

    sp.Popen(
        [
            ivshmem_server_path,
            "--socket-path", ivshmem_server_sock,
            "--memory-path", cxl_path,
            "--memory-size", f"{shmem_size_mb}M",
            "--vector-count", str(msi_vectors),
            "--vm-count", str(num_vms),
            "--vm-offset",
        ],
        stdout=sys.stdout,
        stderr=sys.stderr,
        encoding='utf8'
    )

    return ivshmem_server_sock
