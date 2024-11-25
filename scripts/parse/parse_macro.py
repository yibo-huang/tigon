#!/usr/bin/env python3.11

import os
import copy
import common
import sys
import csv

REMOTE_RATIOS = [(0, 0), (10, 15), (20, 30), (30, 45), (40, 60), (50, 75), (60, 90)]

CROSS_RATIOS = list(range(0, 101, 10))


def main():
    args = common.cli().parse_args()
    res_dir = args.res_dir + "/macro"
    benchmarks = args.benchmark if len(args.benchmark) > 0 else ["tpcc", "smallbank"]

    tpcc = parse_tpcc_remote_txn_overhead(res_dir)
    smallbank = parse_smallbank_remote_txn_overhead(res_dir)

    for benchmark in benchmarks:
        match (benchmark, args.dump):
            case ("tpcc", True):
                common.dump_experiments(tpcc)
            case ("tpcc", False):
                emit_tpcc_remote_txn_overhead(tpcc, res_dir + "/tpcc.csv")
            case ("smallbank", True):
                common.dump_experiments(smallbank)
            case ("smallbank", False):
                emit_smallbank_remote_txn_overhead(
                    smallbank, os.path.join(res_dir + "/smallbank.csv")
                )
            case (benchmark, _):
                raise ValueError(f"Unrecognized benchmark {benchmark}")


def emit_tpcc_remote_txn_overhead(groups, path):
    # write header row first
    rows = [
        ["Remote_Ratio"]
        + [
            f"{neworder_dist}/{payment_dist}"
            for neworder_dist, payment_dist in REMOTE_RATIOS
        ]
    ]

    # read all the files and construct the row
    for name, group in groups.items():
        rows.append([name] + [experiment.output.total_commit for experiment in group])

    # convert rows into columns
    rows = zip(*rows)

    with open(path, "w") as file:
        csv.writer(file).writerows(rows)


def parse_tpcc_remote_txn_overhead(res_dir: str):
    paths = [
        os.path.join(res_dir, path)
        for path in os.listdir(res_dir)
        if path.endswith(".txt")
    ]

    experiments = []

    for path in paths:
        base = common.Input.parse(path)

        if base.benchmark != common.Benchmark.TPCC:
            continue

        data = None
        with open(path) as file:
            data = file.read()

        for (neworder_dist, payment_dist), log in zip(
            REMOTE_RATIOS, data.split("initializing cxl memory...")[1:]
        ):
            input = copy.deepcopy(base)
            input.neworder_dist = neworder_dist
            input.payment_dist = payment_dist
            output = common.parse_output(log, path)
            experiments.append((input, output))

    # group by name and sort by neworder_dist
    return {
        name: list(
            sorted(
                [
                    common.Experiment(input, output)
                    for input, output in experiments
                    if input.name() == name
                ],
                key=lambda experiment: experiment.input.neworder_dist,
            )
        )
        for name in common.ORDER
    }


def emit_smallbank_remote_txn_overhead(groups, output):
    # write header row first
    rows = [["Remote_Ratio"] + CROSS_RATIOS]

    # read all the files and construct the row
    for name, group in groups.items():
        rows.append([name] + [experiment.output.total_commit for experiment in group])

    # convert rows into columns
    rows = zip(*rows)

    with open(output, "w") as file:
        csv.writer(file).writerows(rows)


def parse_smallbank_remote_txn_overhead(res_dir: str):
    paths = [
        os.path.join(res_dir, path)
        for path in os.listdir(res_dir)
        if path.endswith(".txt")
    ]

    experiments = []

    for path in paths:
        base = common.Input.parse(path)

        if base.benchmark != common.Benchmark.SMALLBANK:
            continue

        data = None
        with open(path) as file:
            data = file.read()

        for cross_ratio, log in zip(
            CROSS_RATIOS, data.split("initializing cxl memory...")[1:]
        ):
            input = copy.deepcopy(base)
            input.cross_ratio = cross_ratio
            output = common.parse_output(log, path)
            experiments.append((input, output))

    # group by name and sort by cross ratio
    return {
        name: list(
            sorted(
                [
                    common.Experiment(input, output)
                    for input, output in experiments
                    if input.name() == name
                ],
                key=lambda experiment: experiment.input.cross_ratio,
            )
        )
        for name in common.ORDER
    }


if __name__ == "__main__":
    main()
