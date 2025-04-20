import os

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
LINUX_VM_BUILD_DIR=os.path.join(SCRIPT_DIR, "..", "linux_qemu", "setup_vm")
IMG_NAME = "root.img"
DEFAULT_DRIVE_PATH = os.path.join(LINUX_VM_BUILD_DIR, "qbuild", IMG_NAME)
DEFAULT_DRIVE_OVMF_PATH = os.path.join(LINUX_VM_BUILD_DIR, "qbuild_ovmf", IMG_NAME)
DEFAULT_VM_DIR = os.path.join(SCRIPT_DIR, "..", "vms")
