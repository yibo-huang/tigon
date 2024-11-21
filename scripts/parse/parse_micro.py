#!/usr/bin/env python3

import parse
import sys
import csv
import os


ORDER = {
    name: index
    for index, name in enumerate(
        [
            "Tigon",
            "Sundial-CXL-improved",
            "Sundial-CXL",
            "Sundial-NET",
            "TwoPL-CXL-improved",
            "TwoPL-CXL",
            "TwoPL-NET",
        ]
    )
}


def get_row(input):
    # tput, CXL_usage_index, CXL_usage_data, CXL_usage_transport
    row = [input.name()]

    for cross_ratio in input.data.keys():
        data = input.data[cross_ratio]

        row.append(data["total_commit"])
    return row


def emit(inputs, output):
    row = list()
    rows = list()

    # write header row first
    rows.append(["Remote_Ratio"] + list(inputs[0].data.keys()))

    # read all the files and construct the row
    for input in inputs:
        row = get_row(input)
        rows.append(row)

    # convert rows into columns
    rows = zip(*rows)

    with open(output, "w") as file:
        csv.writer(file).writerows(rows)


def parse_ycsb_remote_txn_overhead(res_dir, rw_ratio, zipf_theta):
    output = os.path.join(res_dir, f"ycsb-micro-{rw_ratio}-{zipf_theta}.csv")
    paths = [
        os.path.join(res_dir, path)
        for path in os.listdir(res_dir)
        if path.endswith(".txt")
    ]

    logs = sorted(
        filter(
            lambda log: log.rw_ratio == rw_ratio
            and log.zipf_theta == zipf_theta
            and log.workload == "rmw",
            map(parse.Log, paths),
        ),
        key=lambda log: ORDER[log.name()],
    )

    emit(list(logs), output)


if len(sys.argv) != 2:
    print("Usage: " + sys.argv[0] + " res_dir")
    sys.exit(-1)

res_dir = sys.argv[1] + "/micro"

parse_ycsb_remote_txn_overhead(res_dir, 100, 0)
parse_ycsb_remote_txn_overhead(res_dir, 0, 0)
