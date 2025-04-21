import os

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = os.path.join(SCRIPT_DIR, "config")
LINUX_VM_BUILD_DIR = os.path.join(SCRIPT_DIR, "..", "image")
IMG_NAME = "root.img"
DEFAULT_DRIVE_PATH = os.path.join(LINUX_VM_BUILD_DIR, IMG_NAME)
DEFAULT_DRIVE_OVMF_PATH = os.path.join(LINUX_VM_BUILD_DIR, IMG_NAME)
DEFAULT_VM_DIR = os.path.join(SCRIPT_DIR, "..", "vms")
