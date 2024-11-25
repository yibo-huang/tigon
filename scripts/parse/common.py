import argparse
from enum import auto, StrEnum
import re
import os
import sys

ORDER = [
    "Tigon",
    "Sundial-CXL-improved",
    "Sundial-CXL",
    "Sundial-NET",
    "TwoPL-CXL-improved",
    "TwoPL-CXL",
    "TwoPL-NET",
]


class Protocol(StrEnum):
    SUNDIAL_PASHA = "SundialPasha"
    SUNDIAL = "Sundial"
    TWO_PL_PASHA = "TwoPLPasha"
    TWO_PL = "TwoPL"


class Benchmark(StrEnum):
    TPCC = auto()
    YCSB = auto()
    SMALLBANK = auto()


class TpccWorkload(StrEnum):
    MIXED = auto()
    NEWORDER = auto()
    PAYMENT = auto()


class YcsbWorkload(StrEnum):
    MIXED = auto()
    RMW = auto()
    SCAN = auto()


class MigrationPolicy(StrEnum):
    ON_DEMAND_FIFO = "OnDemandFIFO"
    EAGERLY = "Eagerly"
    NO_MOVE_OUT = "NoMoveOut"
    LRU = "LRU"


class WhenToMoveOut(StrEnum):
    ON_DEMAND = "OnDemand"
    REACTIVE = "Reactive"


class SccMechanism(StrEnum):
    NON_TEMPORAL = "NonTemporal"
    NO_OP = "NoOP"
    WRITE_THROUGH = "WriteThrough"


class PreMigrate(StrEnum):
    NONE = "None"
    ALL = "All"
    NONPART = "NonPart"


class LoggingType(StrEnum):
    WAL = "WAL"
    GROUP_WAL = "GROUP_WAL"
    BLACKHOLE = "BLACKHOLE"


class Input:
    def __init__(
        self,
        benchmark: Benchmark,
        protocol: str,
        host_num: int,
        worker_num: int,
        use_cxl_trans: bool,
        use_output_thread: bool,
        migration_policy: MigrationPolicy,
        when_to_move_out: WhenToMoveOut,
        max_migrated_rows_size: int,
        scc_mechanism: SccMechanism,
        pre_migrate: PreMigrate,
        logging_type: LoggingType,
    ):
        self.benchmark = benchmark
        self.protocol = protocol
        self.host_num = host_num
        self.worker_num = worker_num
        self.use_cxl_trans = use_cxl_trans
        self.use_output_thread = use_output_thread
        self.migration_policy = migration_policy
        self.when_to_move_out = when_to_move_out
        self.max_migrated_rows_size = max_migrated_rows_size
        self.scc_mechanism = scc_mechanism
        self.logging_type = logging_type

    def parse(path: str):
        name = os.path.basename(path)
        if name.startswith("tpcc"):
            return TpccInput.parse(name)
        elif name.startswith("ycsb"):
            return YcsbInput.parse(name)
        elif name.startswith("smallbank"):
            return SmallbankInput.parse(name)
        else:
            raise ValueError(f"Unknown benchmark name {name.split('-')[0]}")

    def __repr__(self):
        return (
            "Input { " + ", ".join([f"{k}: {v}" for k, v in vars(self).items()]) + " }"
        )

    def name(self):
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


class TpccInput(Input):
    def __init__(
        self,
        *args,
        workload: TpccWorkload = TpccWorkload.MIXED,
        neworder_dist: int = 0,
        payment_dist: int = 0,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.workload = workload
        self.neworder_dist = neworder_dist
        self.payment_dist = payment_dist

    def parse(path: str):
        args = os.path.basename(path).rstrip(".txt").split("-")
        params = [
            ("benchmark", Benchmark),
            ("protocol", Protocol),
            ("host_num", int),
            ("worker_num", int),
            ("use_cxl_trans", lambda value: value == "1"),
            ("use_output_thread", lambda value: value == "1"),
            ("migration_policy", MigrationPolicy),
            ("when_to_move_out", WhenToMoveOut),
            ("max_migrated_rows_size", int),
            ("scc_mechanism", SccMechanism),
            ("pre_migrate", PreMigrate),
            ("logging_type", LoggingType),
        ]
        return TpccInput(**{k: parse(v) for v, (k, parse) in zip(args, params)})


class YcsbInput(Input):
    def __init__(
        self,
        *args,
        workload: YcsbWorkload,
        read_write_ratio: int,
        cross_ratio: int = 0,
        zipf_theta: float,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.workload = workload
        self.read_write_ratio = read_write_ratio
        self.cross_ratio = cross_ratio
        self.zipf_theta = zipf_theta

    def parse(path: str):
        args = os.path.basename(path).rstrip(".txt").split("-")
        params = [
            ("benchmark", Benchmark),
            ("protocol", Protocol),
            ("workload", YcsbWorkload),
            ("host_num", int),
            ("worker_num", int),
            ("read_write_ratio", int),
            ("zipf_theta", float),
            ("use_cxl_trans", lambda value: value == "1"),
            ("use_output_thread", lambda value: value == "1"),
            ("migration_policy", MigrationPolicy),
            ("when_to_move_out", WhenToMoveOut),
            ("max_migrated_rows_size", int),
            ("scc_mechanism", SccMechanism),
            ("pre_migrate", PreMigrate),
            ("logging_type", LoggingType),
        ]
        return YcsbInput(**{k: parse(v) for v, (k, parse) in zip(args, params)})


class SmallbankInput(Input):
    def __init__(
        self,
        *args,
        keys: int,
        cross_ratio: int = 0,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.keys = keys
        self.cross_ratio = cross_ratio

    def parse(path: str):
        args = os.path.basename(path).rstrip(".txt").split("-")
        params = [
            ("benchmark", Benchmark),
            ("protocol", Protocol),
            ("host_num", int),
            ("worker_num", int),
            ("keys", int),
            ("use_cxl_trans", lambda value: value == "1"),
            ("use_output_thread", lambda value: value == "1"),
            ("migration_policy", MigrationPolicy),
            ("when_to_move_out", WhenToMoveOut),
            ("max_migrated_rows_size", int),
            ("scc_mechanism", SccMechanism),
            ("pre_migrate", PreMigrate),
            ("logging_type", LoggingType),
        ]
        return SmallbankInput(**{k: parse(v) for v, (k, parse) in zip(args, params)})


def capture(name: str) -> str:
    # Any sequence of non-whitespace characters, to handle floating point
    # numbers like 3e06 or 1.2.
    NUMBER = "[^\\s,]+"
    return f"{name}: (?P<{name}>{NUMBER})"


CAPTURES = [
    re.compile(
        " ".join(
            ["local CXL memory usage:"]
            + list(
                map(
                    capture,
                    [
                        "size_index_usage",
                        "size_metadata_usage",
                        "size_data_usage",
                        "size_transport_usage",
                    ],
                )
            )
        )
    ),
    re.compile(
        " ".join(
            ["Global Stats:"]
            + list(
                map(
                    capture,
                    [
                        "total_commit",
                        "total_size_index_usage",
                        "total_size_metadata_usage",
                        "total_size_data_usage",
                        "total_size_transport_usage",
                    ],
                )
            )
        )
    ),
    re.compile(capture("abort_rate")),
]


class Output:
    def __init__(
        self,
        size_index_usage: float,
        size_metadata_usage: float,
        size_data_usage: float,
        size_transport_usage: float,
        total_commit: int,
        total_size_index_usage: float,
        total_size_metadata_usage: float,
        total_size_data_usage: float,
        total_size_transport_usage: float,
        abort_rate: float,
    ):
        self.size_index_usage = size_index_usage
        self.size_metadata_usage = size_metadata_usage
        self.size_data_usage = size_data_usage
        self.size_transport_usage = size_transport_usage
        self.total_commit = total_commit
        self.total_size_index_usage = total_size_index_usage
        self.total_size_metadata_usage = total_size_metadata_usage
        self.total_size_data_usage = total_size_data_usage
        self.total_size_transport_usage = total_size_transport_usage
        self.abort_rate = abort_rate

    def parse(data: str):
        union = {}
        for capture in CAPTURES:
            union |= capture.search(data).groupdict()
        return Output(**{k: float(v) for k, v in union.items()})


def parse_cli():
    parser = argparse.ArgumentParser(
        description="Parse Pasha logs for benchmarks",
    )
    parser.add_argument("res_dir", help="Path to results directory")
    parser.add_argument("--dump", action="store_true", help="Dump statistics to stdout")
    parser.add_argument(
        "-b",
        "--benchmark",
        action="append",
        help="Select benchmark to parse",
        default=[],
    )
    return parser.parse_args()


if __name__ == "__main__":
    log = sys.argv[1]
    print(log)
