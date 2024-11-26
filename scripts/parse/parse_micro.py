#!/usr/bin/env python3.11

import common
import copy
import csv
import os
import sys


def main():
    args = common.cli().parse_args()
    print(f"Parsing results from {args.res_dir}...", file=sys.stderr)
    res_dir = args.res_dir

    if args.dump:
        groups = parse_ycsb_remote_txn_overhead(res_dir)
        common.dump_experiments(groups)

    else:
        for rw_ratio in [80]:
            for zipf_theta in [0.99, 0.7]:
                groups = parse_ycsb_remote_txn_overhead(
                    res_dir, filter(rw_ratio, zipf_theta, common.YcsbWorkload.RMW)
                )

                emit_ycsb_remote_txn_overhead(
                    groups,
                    os.path.join(res_dir, f"ycsb-micro-{rw_ratio}-{zipf_theta}.csv"),
                )
                emit_ycsb_hwcc_overhead(
                    groups,
                    os.path.join(
                        res_dir, f"ycsb-micro-hwcc-{rw_ratio}-{zipf_theta}.csv"
                    ),
                )


def filter(rw_ratio: int, zipf_theta: float, workload: common.YcsbWorkload):
    return lambda input: (
        input.benchmark == common.Benchmark.YCSB
        and input.read_write_ratio == rw_ratio
        and abs(input.zipf_theta - zipf_theta) < 1e-5
        and input.workload == workload
    )


def emit_ycsb_remote_txn_overhead(groups, output):
    # write header row first
    rows = [["Remote_Ratio"] + common.CROSS_RATIOS]

    # read all the files and construct the row
    for name, group in groups.items():
        rows.append([name] + [experiment.output.total_commit for experiment in group])

    # convert rows into columns
    rows = zip(*rows)

    with open(output, "w") as file:
        csv.writer(file).writerows(rows)


def emit_ycsb_hwcc_overhead(groups, output):
    # write header row first
    rows = [["Remote_Ratio"] + common.CROSS_RATIOS]

    # read all the files and construct the row
    for name, group in groups.items():
        # HWCC size only relevant to Tigon
        if name == "Tigon":
            for size in common.HWCC_SIZES:
                row = [size]
                # O(n^2) lookup here but should only be a few experiments
                for cross_ratio in common.CROSS_RATIOS:
                    for experiment in group:
                        if (
                            experiment.input.max_migrated_rows_size == size
                            and experiment.input.cross_ratio == cross_ratio
                        ):
                            row.append(experiment.output.total_commit)
                            break
                rows.append(row)
        else:
            rows.append(
                [name] + [experiment.output.total_commit for experiment in group]
            )

    # convert rows into columns
    rows = zip(*rows)

    with open(output, "w") as file:
        csv.writer(file).writerows(rows)


def parse_ycsb_remote_txn_overhead(res_dir, filter=lambda input: True):
    paths = [
        os.path.join(res_dir, path)
        for path in os.listdir(res_dir)
        if path.endswith(".txt")
    ]

    if len(paths) == 0:
        print(f"Failed to find logs in {res_dir}", file=sys.stderr)
        sys.exit(1)

    experiments = []
    names = set()

    for path in paths:
        base = common.Input.parse(path)
        names.add(base.name())

        if not filter(base):
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
