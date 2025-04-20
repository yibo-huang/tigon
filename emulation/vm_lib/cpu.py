from enum import Enum
from run_command import run_local_command


class CPUKind(Enum):
    Intel = 1
    AMD_EPYC = 2


def get_cpu_model():
    ret = run_local_command(['lscpu'])
    output = ret.stdout.splitlines()
    for line in output:
        if "Vendor ID" in line:
            print(line)
            left, right = line.split(":", 1)
            left = left.strip()
            right = right.strip()
            if "AMD" in right:
                continue
            elif "Intel" in right:
                return CPUKind.Intel
        if "Model name" in line:
            print(line)
            left, right = line.split(":", 1)
            left = left.strip()
            right = right.strip()
            if "EPYC" in right:
                # double check it's actually a epyc amd cpu
                return CPUKind.AMD_EPYC
