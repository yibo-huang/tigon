#!/usr/bin/env python3.11

from parse import Log
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


def dump(inputs):
    out = csv.writer(sys.stdout)

    # write header row first
    out.writerow(["system", "remote_ratio"] + list(inputs[0].data[0].keys()))

    for input in inputs:
        for cross_ratio, data in input.data.items():
            # write header row first
            out.writerow([input.name(), cross_ratio] + list(data.values()))


def parse(res_dir):
    paths = [
        os.path.join(res_dir, path)
        for path in os.listdir(res_dir)
        if path.endswith(".txt")
    ]

    logs = sorted(
        map(Log, paths),
        key=lambda log: ORDER[log.name()],
    )

    dump(list(logs))


if len(sys.argv) != 2:
    print("Usage: " + sys.argv[0] + " res_dir")
    sys.exit(-1)


parse(sys.argv[1])
