# ivshmem

Split into three directories:

- `ivshmem-kernel` contains an out-of-tree kernel module for
  an interrupt-capable PCI device driver. Interrupts are sent
  via `pwrite` and can be blocked via `pread` on the character
  device (`ivpci`).

- `ivshmem-host` contains a Rust port of the
  [QEMU `ivshmem-server` and `ivshmem-client` binaries](https://github.com/qemu/qemu/tree/master/contrib).

- `ivshmem-user` contains some benchmarking utilities for
  measuring end-to-end interrupt latency.
