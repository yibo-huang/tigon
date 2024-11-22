#!/usr/bin/env python3.11

import copy
import common
import sys
import csv
import os


CROSS_RATIOS = list(range(0, 101, 10))


def dump_ycsb_remote_txn_overhead(experiments):
    rows = []

    # write header row first
    rows.append(["system", "remote_ratio"] + list(vars(experiments[0][1]).keys()))

    # group by name and sort by cross ratio
    groups = {
        name: list(
            sorted(
                [
                    (input, output)
                    for input, output in experiments
                    if input.name() == name
                ],
                key=lambda key: key[0].cross_ratio,
            )
        )
        for name in common.ORDER
    }

    for name, group in groups.items():
        for input, output in group:
            # dump all captured outputs
            rows.append([name, input.cross_ratio] + list(vars(output).values()))

    csv.writer(sys.stdout).writerows(rows)


def emit_ycsb_remote_txn_overhead(experiments, output):
    rows = []

    # write header row first
    rows.append(["Remote_Ratio"] + CROSS_RATIOS)

    # group by name and sort by cross ratio
    groups = {
        name: list(
            sorted(
                [
                    (input, output)
                    for input, output in experiments
                    if input.name() == name
                ],
                key=lambda key: key[0].cross_ratio,
            )
        )
        for name in common.ORDER
    }

    # read all the files and construct the row
    for name, group in groups.items():
        rows.append([name] + [output.total_commit for _, output in group])

    # convert rows into columns
    rows = zip(*rows)

    with open(output, "w") as file:
        csv.writer(file).writerows(rows)


def parse_ycsb_remote_txn_overhead(res_dir, rw_ratio, zipf_theta):
    paths = [
        os.path.join(res_dir, path)
        for path in os.listdir(res_dir)
        if path.endswith(".txt")
    ]

    experiments = []

    for path in paths:
        base = common.Input.parse(path)

        if (
            base.read_write_ratio != rw_ratio
            or base.zipf_theta != zipf_theta
            or base.workload != common.YcsbWorkload.RMW
        ):
            continue

        data = None
        with open(path) as file:
            data = file.read()

        for cross_ratio, log in zip(
            CROSS_RATIOS, data.split("initializing cxl memory...")[1:]
        ):
            args = copy.deepcopy(base)
            args.cross_ratio = cross_ratio
            output = common.Output.parse(log)
            experiments.append((args, output))

    emit_ycsb_remote_txn_overhead(
        experiments,
        os.path.join(res_dir, f"ycsb-micro-{rw_ratio}-{zipf_theta}.csv"),
    )


if len(sys.argv) != 2:
    print("Usage: " + sys.argv[0] + " res_dir")
    sys.exit(-1)

res_dir = sys.argv[1] + "/micro"

parse_ycsb_remote_txn_overhead(res_dir, 100, 0)
parse_ycsb_remote_txn_overhead(res_dir, 0, 0)
