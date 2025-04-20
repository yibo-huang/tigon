from ivshmem import setup_cxl_host, start_ivshmem

vmdir = "./"
cxl_host_dir = "/mnt/cxl_mem"
shmem_size_mb = 4096
msi_vectors = 6
cxl_host_dir_size_mb = shmem_size_mb + 512

setup_cxl_host(cxl_host_dir, cxl_host_dir_size_mb)
start_ivshmem(vmdir, cxl_host_dir, shmem_size_mb, msi_vectors, False)