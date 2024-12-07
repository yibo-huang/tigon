#!/usr/bin/env python3.11

import os
import copy
import common
import sys
import polars as pl


def parse(res_dir: str):
    paths = sorted(
        [
            os.path.join(res_dir, path)
            for path in os.listdir(res_dir)
            if path.endswith(".txt")
        ]
    )

    if len(paths) == 0:
        print(f"Failed to find logs in {res_dir}", file=sys.stderr)
        sys.exit(1)

    experiments = []

    for path in paths:
        base = common.Input.parse(path)
        data = None

        with open(path) as file:
            data = file.read()

        logs = data.split("initializing cxl memory...")[1:]

        match base.benchmark:
            case common.Benchmark.TPCC:
                for (neworder_dist, payment_dist), log in zip(
                    common.REMOTE_RATIOS, logs
                ):
                    input = copy.deepcopy(base)
                    input.neworder_dist = neworder_dist
                    input.payment_dist = payment_dist
                    output = common.parse_output(log, path)
                    experiments.append(common.frame(input, output))
            case common.Benchmark.YCSB:
                for cross_ratio, log in zip(common.CROSS_RATIOS, logs):
                    input = copy.deepcopy(base)
                    input.cross_ratio = cross_ratio
                    output = common.parse_output(log, path)
                    experiments.append(common.frame(input, output))

    return pl.concat(experiments, how="diagonal")


if __name__ == "__main__":
    main()
