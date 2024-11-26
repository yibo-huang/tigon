#!/usr/bin/env python3.11

import os
import copy
import common
import csv


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
                emit_tpcc_hwcc_overhead(tpcc, res_dir + "/tpcc-hwcc.csv")
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
            for neworder_dist, payment_dist in common.REMOTE_RATIOS
        ]
    ]

    # read all the files and construct the row
    for name, group in groups.items():
        rows.append([name] + [experiment.output.total_commit for experiment in group])

    # convert rows into columns
    rows = zip(*rows)

    with open(path, "w") as file:
        csv.writer(file).writerows(rows)


def emit_tpcc_hwcc_overhead(groups, path):
    rows = [
        ["Remote_Ratio"]
        + [
            f"{neworder_dist}/{payment_dist}"
            for neworder_dist, payment_dist in common.REMOTE_RATIOS
        ]
    ]

    for name, group in groups.items():
        if name == "Tigon":
            for size in common.HWCC_SIZES:
                row = [size]

                # O(n^2) lookup here but should only be a few experiments
                for neworder_dist, payment_dist in common.REMOTE_RATIOS:
                    for experiment in group:
                        if (
                            experiment.input.max_migrated_rows_size == size
                            and experiment.input.neworder_dist == neworder_dist
                            and experiment.input.payment_dist == payment_dist
                        ):
                            row.append(experiment.output.total_commit)
                            break

                rows.append(row)
        else:
            rows.append(
                [name] + [experiment.output.total_commit for experiment in group]
            )

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
    names = set()

    for path in paths:
        base = common.Input.parse(path)
        names.add(base.name())

        if base.benchmark != common.Benchmark.TPCC:
            continue

        data = None
        with open(path) as file:
            data = file.read()

        for (neworder_dist, payment_dist), log in zip(
            common.REMOTE_RATIOS, data.split("initializing cxl memory...")[1:]
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
        if name in names
    }


def emit_smallbank_remote_txn_overhead(groups, output):
    # write header row first
    rows = [["Remote_Ratio"] + common.CROSS_RATIOS]

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
    names = set()

    for path in paths:
        base = common.Input.parse(path)
        names.add(base.name())

        if base.benchmark != common.Benchmark.SMALLBANK:
            continue

        data = None
        with open(path) as file:
            data = file.read()

        for cross_ratio, log in zip(
            common.CROSS_RATIOS, data.split("initializing cxl memory...")[1:]
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
        if name in names
    }


if __name__ == "__main__":
    main()
