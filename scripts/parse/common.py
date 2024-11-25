from typing import Optional, TypeAlias
from types import SimpleNamespace
import argparse
import csv
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


def capture(
    name: str,
    rename: Optional[str] = None,
    sep: str = ": ",
    prefix: str = "",
    suffix: str = "",
) -> str:
    rename = rename or name
    rename = rename.replace(" ", "_")

    # Any sequence of non-whitespace characters, to handle floating point
    # numbers like 3e06 or 1.2.
    NUMBER = "[^\\s,%]+"
    return f"{prefix}{name}{sep}(?P<{rename}>{NUMBER}){suffix}"


CAPTURES = {
    # Name is used only for debugging
    name: re.compile(pattern)
    for name, pattern in {
        "Local CXL": " ".join(
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
        ),
        "Global": " ".join(
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
        ),
        "Coordinator": ", ".join(
            [
                capture("abort_rate"),
                capture("network size"),
                capture("avg network size"),
                capture("si_in_serializable")
                + capture(
                    "", rename="si_in_serializable_pct", prefix=" ", sep="", suffix=" %"
                ),
                capture("local", rename="local_pct", suffix=" %"),
                capture("local_access"),
                capture("local_cxl_access")
                + capture(
                    "",
                    rename="local_cxl_acccess_pct",
                    sep="",
                    prefix=" \\(",
                    suffix="%\\)",
                ),
                capture("remote_access"),
                capture("remote_access_with_req")
                + capture(
                    "",
                    rename="remote_access_with_req_pct",
                    sep="",
                    prefix=" \\(",
                    suffix="%\\)",
                ),
                capture("data_move_in"),
                capture("data_move_out"),
            ]
        ),
        "WALLogger": " ".join(
            list(
                map(
                    lambda key: capture(key, sep=" "),
                    [
                        "committed_txn_cnt",
                        "disk_sync_cnt",
                        "disk_sync_size",
                    ],
                )
            )
        ),
        # Note: per worker
        "Worker": "Worker \\d+ latency: "
        + " ".join(
            [
                capture("", rename="worker_lat_p50", sep="", suffix=" us \\(50%\\)"),
                capture("", rename="worker_lat_p75", sep="", suffix=" us \\(75%\\)"),
                capture("", rename="worker_lat_p95", sep="", suffix=" us \\(95%\\)"),
                capture("", rename="worker_lat_p99", sep="", suffix=" us \\(99%\\)."),
                # dist
                capture(
                    "dist txn latency",
                    rename="worker_lat_dist_p50",
                    suffix=" us \\(50%\\)",
                ),
                capture(
                    "", rename="worker_lat_dist_p75", sep="", suffix=" us \\(75%\\)"
                ),
                capture(
                    "", rename="worker_lat_dist_p95", sep="", suffix=" us \\(95%\\)"
                ),
                capture(
                    "", rename="worker_lat_dist_p99", sep="", suffix=" us \\(99%\\)"
                ),
                capture(
                    "avg",
                    rename="worker_lat_dist_avg",
                    sep=" ",
                    prefix=" ",
                    suffix=" us .",
                ),
                # local
                capture(
                    "local txn latency",
                    rename="worker_lat_local_p50",
                    suffix=" us \\(50%\\)",
                ),
                capture(
                    "", rename="worker_lat_local_p75", sep="", suffix=" us \\(75%\\)"
                ),
                capture(
                    "", rename="worker_lat_local_p95", sep="", suffix=" us \\(95%\\)"
                ),
                capture(
                    "", rename="worker_lat_local_p99", sep="", suffix=" us \\(99%\\)"
                ),
                capture(
                    "avg",
                    rename="worker_lat_local_avg",
                    sep=" ",
                    prefix=" ",
                    suffix=" us.",
                ),
                # commit
                capture(
                    "txn commit latency",
                    rename="worker_lat_commit_p50",
                    suffix=" us \\(50%\\)",
                ),
                capture(
                    "", rename="worker_lat_commit_p75", sep="", suffix=" us \\(75%\\)"
                ),
                capture(
                    "", rename="worker_lat_commit_p95", sep="", suffix=" us \\(95%\\)"
                ),
                capture(
                    "", rename="worker_lat_commit_p99", sep="", suffix=" us \\(99%\\)"
                ),
                capture(
                    "avg",
                    rename="worker_lat_commit_avg",
                    sep=" ",
                    suffix=" us.",
                ),
            ]
        ),
        "LOCAL": "LOCAL "
        + " us,  ".join(
            list(
                map(
                    lambda key: capture(key, rename=f"worker_local_{key}", sep=" "),
                    [
                        "txn stall",
                        "local_work",
                        "remote_work",
                        "commit_work",
                        "commit_prepare",
                        "commit_persistence",
                        "commit_write_back",
                        "commit_replication",
                        "commit_release_lock",
                    ],
                )
            )
        ),
        # Note: per worker
        "DIST": "DIST "
        + " us,  ".join(
            list(
                map(
                    lambda key: capture(key, rename=f"worker_dist_{key}", sep=" "),
                    [
                        "txn stall",
                        "local_work",
                        "remote_work",
                        "commit_work",
                        "commit_prepare",
                        "commit_persistence",
                        "commit_write_back",
                        "commit_replication",
                        "commit_release_lock",
                    ],
                )
            )
        ),
        "SCCManager": "software cache-coherence statistics: "
        + " ".join(
            [
                capture("num_clflush"),
                capture("num_clwb"),
                capture("num_cache_hit"),
                capture("num_cache_miss"),
                capture("cache hit rate", rename="cache_hit_rate_pct", suffix="%"),
            ]
        ),
        "Round trip": " ".join(
            [
                capture(
                    "round_trip_latency",
                    rename="round_trip_lat_p50",
                    sep=" ",
                    suffix=" \\(50th\\)",
                ),
                capture("", rename="round_trip_lat_p75", sep="", suffix=" \\(75th\\)"),
                capture("", rename="round_trip_lat_p95", sep="", suffix=" \\(95th\\)"),
                capture("", rename="round_trip_lat_p99", sep="", suffix=" \\(99th\\)"),
            ]
        ),
    }.items()
}


Output: TypeAlias = SimpleNamespace


def parse_output(data: str, path: str = None) -> Output:
    union = {}
    for name, regex in CAPTURES.items():
        matches = regex.search(data)
        if matches is None:
            print(f"Failed to match {name} regex in {path}", file=sys.stderr)
            union |= {name: float("NaN") for name in regex.groupindex}
            continue
        union |= matches.groupdict()
    return Output(**{k: float(v) for k, v in union.items()})


class Experiment:
    def __init__(
        self,
        input: Input,
        output: Output,
    ):
        self.input = input
        self.output = output

    def keys(self):
        return list(vars(self.input).keys()) + list(vars(self.output).keys())

    def values(self):
        return list(vars(self.input).values()) + list(vars(self.output).values())


def dump_experiments(groups: dict[str, list[Experiment]]):
    out = csv.writer(sys.stdout)
    first = True

    # read all the files and construct the row
    for name, group in groups.items():
        for experiment in group:
            if first:
                first = False
                out.writerow(["system"] + experiment.keys())

            out.writerow([name] + experiment.values())


def cli() -> argparse.ArgumentParser:
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
    return parser


if __name__ == "__main__":
    log = sys.argv[1]
    print(log)
