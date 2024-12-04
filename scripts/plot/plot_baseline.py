#!/usr/bin/env python3

import argparse
from enum import auto, StrEnum
import math
from pathlib import PurePath

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

import common


MARKER_SIZE = 10.0
MARKER_EDGE_WIDTH = 1.6
MARKER_FACE_COLOR = "none"
LINEWIDTH = 1.0


class Experiment(StrEnum):
    # Baseline optimizations
    BASELINE = auto()
    # Custom YCSB workload
    CUSTOM = auto()
    # No modifier
    DEFAULT = auto()
    # Hardware cache coherence sensitivity
    HWCC = auto()


def main():
    args = cli().parse_args()
    input = path(args)
    df = pd.read_csv(input, index_col=0)

    if args.experiment == Experiment.BASELINE:
        baseline(args, df)

    plt.savefig(
        input.with_suffix(".pdf"),
        format="pdf",
        bbox_inches="tight",
    )


def baseline(args, df):
    if args.benchmark == common.Benchmark.YCSB:
        df = df.reindex(list(range(0, 101, 20)))

    xs = df.index

    # Configure axis range
    max_y = df.to_numpy().max()
    max_y_rounded_up = math.ceil(max_y / 200000.0) * 200000.0

    figure, axes = plt.subplots(nrows=1, ncols=2, sharey=True)
    figure.set_size_inches(w=6, h=3)
    figure.set_layout_engine("constrained")
    figure.supxlabel(
        "Multi-partition Transaction Percentage"
        + (" (NewOrder/Payment)" if args.benchmark == common.Benchmark.TPCC else "")
    )
    figure.supylabel("Throughput (txns/sec)")

    # Plot Sundial on left
    for (label, row), marker in zip(
        df.T.loc[["Sundial-CXL-improved", "Sundial-CXL", "Sundial-NET"]].iterrows(),
        ["^", "s", "o"],
    ):
        axes[0].plot(
            xs,
            row,
            color="#FFC003",
            marker=marker,
            markersize=MARKER_SIZE,
            linewidth=LINEWIDTH,
            markeredgewidth=MARKER_EDGE_WIDTH,
            markerfacecolor=MARKER_FACE_COLOR,
            label=label,
        )

    # Plot TwoPL on right
    for (label, row), marker in zip(
        df.T.loc[["TwoPL-CXL-improved", "TwoPL-CXL", "TwoPL-NET"]].iterrows(),
        ["^", "s", "o"],
    ):
        axes[1].plot(
            xs,
            row,
            color="#4372C4",
            marker=marker,
            markersize=MARKER_SIZE,
            linewidth=LINEWIDTH,
            markeredgewidth=MARKER_EDGE_WIDTH,
            mfc=MARKER_FACE_COLOR,
            label=label,
        )

    for axis in axes:
        # Must be after plotting
        # https://stackoverflow.com/questions/22642511/change-y-range-to-start-from-0-with-matplotlib
        axis.set_ylim(bottom=0, top=max_y_rounded_up)

        axis.tick_params(axis="x", labelsize=8.0)
        axis.grid(axis="y")

        formatter = None
        if max_y > 1e6:
            formatter = ticker.FuncFormatter(
                lambda y, pos: "0" if y == 0 else f"{y / 1e6:.1f}M"
            )
        else:
            formatter = ticker.FuncFormatter(
                lambda y, pos: "0" if y == 0 else f"{int(y / 1e3)}K"
            )

        axis.yaxis.set_major_formatter(formatter)
        axis.legend(
            loc="upper right",
            frameon=False,
            fancybox=False,
            framealpha=1,
        )


def path(args) -> PurePath:
    csv = None
    match args.experiment, args.benchmark:
        case Experiment.BASELINE, common.Benchmark.YCSB:
            csv = PurePath(
                "micro", f"baseline-ycsb-{args.rw_ratio}-{args.zipf_theta}.csv"
            )
        case Experiment.BASELINE, common.Benchmark.TPCC:
            csv = PurePath("macro", "baseline-tpcc.csv")

        case Experiment.DEFAULT, common.Benchmark.YCSB:
            csv = PurePath("micro", f"ycsb-{args.rw_ratio}-{args.zipf_theta}.csv")
        case Experiment.DEFAULT, common.Benchmark.TPCC:
            csv = PurePath("macro", "tpcc.csv")
    return PurePath(args.res_dir, csv)


def cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="benchmark")

    subparsers.add_parser(common.Benchmark.TPCC)

    ycsb = subparsers.add_parser(common.Benchmark.YCSB)
    ycsb.add_argument("-z", "--zipf-theta")
    ycsb.add_argument("-r", "--rw-ratio")

    parser.add_argument("-e", "--experiment", default=Experiment.DEFAULT)
    parser.add_argument("res_dir")
    return parser


if __name__ == "__main__":
    main()
