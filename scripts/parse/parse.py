from enum import Enum
import os
import re
import sys

# Any sequence of non-whitespace characters, to handle floating point
# numbers like 3e06 or 1.2.
NUMBER = "[^\\s]+"
LOCAL = re.compile(
    " ".join(
        ["local CXL memory usage:"]
        + [
            f"{name}: (?P<{name}>{NUMBER})"
            for name in [
                "size_index_usage",
                "size_metadata_usage",
                "size_data_usage",
                "size_transport_usage",
            ]
        ]
    )
)
GLOBAL = re.compile(
    " ".join(
        ["Global Stats:"]
        + [
            f"{name}: (?P<{name}>{NUMBER})"
            for name in [
                "total_commit",
                "total_size_index_usage",
                "total_size_metadata_usage",
                "total_size_data_usage",
                "total_size_transport_usage",
            ]
        ]
    )
)


class Log:
    def __init__(self, path: str):
        args = os.path.basename(path).rstrip(".txt").split("-")
        self.benchmark = Benchmark.parse(args[0])
        self.protocol = args[1]
        self.workload = args[2]
        self.host_num = int(args[3])
        self.worker_num = int(args[4])
        self.rw_ratio = int(args[5])
        self.zipf_theta = int(args[6])
        self.use_cxl_trans = args[7] == "1"
        self.use_output_thread = args[8] == "1"
        self.migration_policy = args[9]
        self.when_to_move_out = args[10]
        self.max_migrated_rows_size = int(args[11])
        self.scc_mechanism = args[12]
        self.pre_migrate = args[13]
        self.logging_type = args[14]

        with open(path) as file:
            self.data = Log.parse_all(file.read())

    def __repr__(self):
        return "Log { " + ", ".join([f"{k}: {v}" for k, v in vars(self).items()]) + " }"

    def name(self):
        # FIXME: update for TPCC
        assert self.benchmark == Benchmark.YCSB

        if self.protocol == "TwoPLPasha":
            return "Tigon"

        suffix = None
        match (self.use_cxl_trans, self.use_output_thread):
            case (True, False):
                suffix = "CXL-improved"
            case (True, True):
                suffix = "CXL"
            case (False, True):
                suffix = "NET"
            case (False, False):
                raise ValueError(
                    "Invalid configuration: no CXL transport or output thread"
                )
        return f"{self.protocol}-{suffix}"

    def parse_all(data: str):
        out = {}
        for cross_ratio, data in zip(
            range(0, 101, 10), data.split("initializing cxl memory...\n")[1:]
        ):
            out[cross_ratio] = Log.parse(data)
        return out

    def parse(data: str):
        union = LOCAL.search(data).groupdict() | GLOBAL.search(data).groupdict()
        return {k: int(float(v)) for k, v in union.items()}


class Benchmark(Enum):
    TPCC = 0
    YCSB = 1

    def parse(name: str):
        match name:
            case "tpcc":
                return Benchmark.TPCC
            case "ycsb":
                return Benchmark.YCSB
            case _:
                raise ValueError(f"Unrecognized benchmark name: {name}")


if __name__ == "__main__":
    log = Log(sys.argv[1])
    print(log)
