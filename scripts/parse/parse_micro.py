#!/usr/bin/env python3.11

import common
import copy
import csv
import os


CROSS_RATIOS = list(range(0, 101, 10))


def main():
    args = common.cli().parse_args()
    res_dir = args.res_dir + "/micro"

    if args.dump:
        groups = parse_ycsb_remote_txn_overhead(res_dir)
        common.dump_experiments(groups)

    else:
        for rw_ratio in [100, 0]:
            for zipf_theta in [0.99, 0.7]:
                groups = parse_ycsb_remote_txn_overhead(
                    res_dir, filter(rw_ratio, zipf_theta, common.YcsbWorkload.RMW)
                )

                emit_ycsb_remote_txn_overhead(
                    groups,
                    os.path.join(res_dir, f"ycsb-micro-{rw_ratio}-{zipf_theta}.csv"),
                )


def filter(rw_ratio: int, zipf_theta: float, workload: common.YcsbWorkload):
    return lambda input: (
        input.read_write_ratio == rw_ratio
        and abs(input.zipf_theta - zipf_theta) < 1e-5
        and input.workload == workload
    )


def emit_ycsb_remote_txn_overhead(groups, output):
    # write header row first
    rows = [["Remote_Ratio"] + CROSS_RATIOS]

    # read all the files and construct the row
    for name, group in groups.items():
        rows.append([name] + [experiment.output.total_commit for experiment in group])

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

    experiments = []

    for path in paths:
        base = common.Input.parse(path)

        if not filter(base):
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
