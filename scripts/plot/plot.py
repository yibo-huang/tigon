#!/usr/bin/env python3

import argparse
from enum import auto, StrEnum
import math
from pathlib import PurePath

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

import common


# Default settings
DEFAULT_PLOT = {
    "markersize": 10.0,
    "markeredgewidth": 1.6,
    "markerfacecolor": "none",
    "linewidth": 1.0,
}
DEFAULT_LEGEND = {
    "loc": "upper right",
    "frameon": False,
    "fancybox": False,
    "framealpha": 1,
}
DEFAULT_FIGURE = {
    "layout_engine": "constrained",
}

SUPYLABEL = "Throughput (txns/sec)"


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

    match args.experiment:
        case Experiment.BASELINE:
            baseline(args, df)
        case Experiment.DEFAULT:
            default(args, df)

    plt.savefig(
        input.with_suffix(".pdf"),
        format="pdf",
        bbox_inches="tight",
    )


def default(args, df):
    figure, axis = plt.subplots(nrows=1, ncols=1)

    figure.set(**DEFAULT_FIGURE)
    figure.set_size_inches(w=6, h=4)
    figure.supxlabel(supxlabel(args.benchmark))
    figure.supylabel(SUPYLABEL)

    for system, row in df.T.iterrows():
        axis.plot(
            df.index,
            row,
            color=color(system),
            marker=marker(system),
            label=system,
            **DEFAULT_PLOT,
        )

    max_y = ylim(df)
    axis.set_ylim(bottom=0, top=max_y)
    axis.grid(axis="y")

    axis.yaxis.set_major_formatter(format_yaxis(max_y))
    axis.legend(**DEFAULT_LEGEND)


def baseline(args, df):
    if args.benchmark == common.Benchmark.YCSB:
        df = df.reindex(list(range(0, 101, 20)))

    figure, axes = plt.subplots(nrows=1, ncols=2, sharey=True)

    figure.set(**DEFAULT_FIGURE)
    figure.set_size_inches(w=6, h=3)
    figure.supxlabel(supxlabel(args.benchmark))
    figure.supylabel(SUPYLABEL)

    # Plot Sundial on left
    for system, row in df.T.loc[
        ["Sundial-CXL-improved", "Sundial-CXL", "Sundial-NET"]
    ].iterrows():
        axes[0].plot(
            df.index,
            row,
            color=color(system),
            marker=marker(system),
            label=system,
            **DEFAULT_PLOT,
        )

    # Plot TwoPL on right
    for system, row in df.T.loc[
        ["TwoPL-CXL-improved", "TwoPL-CXL", "TwoPL-NET"]
    ].iterrows():
        axes[1].plot(
            df.index,
            row,
            color=color(system),
            marker=marker(system),
            label=system,
            **DEFAULT_PLOT,
        )

    max_y = ylim(df)

    for axis in axes:
        # Must be after plotting
        # https://stackoverflow.com/questions/22642511/change-y-range-to-start-from-0-with-matplotlib
        axis.set_ylim(bottom=0, top=max_y)

        axis.tick_params(axis="x", labelsize=8.0)
        axis.grid(axis="y")

        axis.yaxis.set_major_formatter(format_yaxis(max_y))
        axis.legend(**DEFAULT_LEGEND)


def color(system: str) -> str:
    if "Sundial" in system:
        return "#FFC003"
    elif "TwoPL" in system:
        return "#4372C4"
    elif "Tigon" in system:
        return "black"


def marker(system: str) -> str:
    if "NET" in system:
        return "o"
    elif "CXL-improved" in system:
        return "^"
    elif "CXL" in system:
        return "s"
    elif "Phantom" in system:
        return "o"
    else:
        return "^"


def supxlabel(benchmark: common.Benchmark) -> str:
    base = "Multi-partition Transaction Percentage"
    if benchmark == common.Benchmark.TPCC:
        base += " (NewOrder/Payment)"
    return base


def format_yaxis(max_y: float) -> ticker.FuncFormatter:
    if max_y > 1e6:
        return ticker.FuncFormatter(lambda y, pos: "0" if y == 0 else f"{y / 1e6:.1f}M")
    else:
        return ticker.FuncFormatter(
            lambda y, pos: "0" if y == 0 else f"{int(y / 1e3)}K"
        )


def ylim(df: pd.DataFrame) -> float:
    max_y = df.to_numpy().max()
    return math.ceil(max_y / 200000.0) * 200000.0


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
