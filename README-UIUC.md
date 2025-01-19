# Setup Environment for the UIUC machine

```sh
# setup the bluefield NIC
/storage/ut_austin/pasha/vhive_setup/linux_qemu/bluefield.sh

# rebooting...
sudo daxctl reconfigure-device --mode=system-ram dax0.0 --force

# launch VMs
sudo ./scripts/setup.sh launch_vms uiuc 8 # we will launch 8 VMs each with 5 cores

# setup VMs
./scripts/setup.sh vms 8
```