#!/usr/bin/env python3.11

import os
import copy
import common
import sys
import csv

REMOTE_RATIOS = [(0, 0), (10, 15), (20, 30), (30, 45), (40, 60), (50, 75), (60, 90)]

CROSS_RATIOS = list(range(0, 101, 10))


def emit_tpcc_remote_txn_overhead(experiments, path):
    # write header row first
    rows = [
        ["Remote_Ratio"]
        + [
            f"{neworder_dist}/{payment_dist}"
            for neworder_dist, payment_dist in REMOTE_RATIOS
        ]
    ]

    # group by name and sort by neworder_dist
    groups = {
        name: list(
            sorted(
                [
                    (input, output)
                    for input, output in experiments
                    if input.name() == name
                ],
                key=lambda experiment: experiment[0].neworder_dist,
            )
        )
        for name in common.ORDER
    }

    # read all the files and construct the row
    for name, group in groups.items():
        rows.append([name] + [output.total_commit for _, output in group])

    # convert rows into columns
    rows = zip(*rows)

    with open(path, "w") as file:
        csv.writer(file).writerows(rows)


def parse_tpcc_remote_txn_overhead(res_dir):
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
            output = common.Output.parse(log)
            experiments.append((input, output))

    emit_tpcc_remote_txn_overhead(experiments, res_dir + "/tpcc.csv")


def emit_smallbank_remote_txn_overhead(inputs, output):
    rows = []

    # write header row first
    rows.append(["Remote_Ratio"] + CROSS_RATIOS)

    # group by name and sort by cross ratio
    groups = {
        name: list(
            sorted(
                [(input, output) for input, output in inputs if input.name() == name],
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


def parse_smallbank_remote_txn_overhead(res_dir):
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
            args = copy.deepcopy(base)
            args.cross_ratio = cross_ratio
            output = common.Output.parse(log)
            experiments.append((args, output))

    emit_smallbank_remote_txn_overhead(experiments, os.path.join(res_dir + "/smallbank.csv"))


if len(sys.argv) != 2:
    print("Usage: " + sys.argv[0] + " res_dir")
    sys.exit(-1)

res_dir = sys.argv[1] + "/macro"

parse_tpcc_remote_txn_overhead(res_dir)
parse_smallbank_remote_txn_overhead(res_dir)
