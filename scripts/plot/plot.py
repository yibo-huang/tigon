#!/usr/bin/env python3

from collections.abc import Iterable
import argparse
from enum import auto, StrEnum
import math
from pathlib import PurePath
import sys

import numpy as np
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
    "framealpha": 1,
}
DEFAULT_FIGURE = {
    "layout_engine": "constrained",
}
DEFAULT_SAVE = {
    "bbox_inches": "tight",
}

SUPYLABEL = "Throughput (txns/sec)"
REDACT = "Tigon"


class Experiment(StrEnum):
    # Baseline optimizations
    BASELINE = auto()
    # Custom YCSB workload
    CUSTOM = auto()
    # No modifier
    DEFAULT = auto()
    # Hardware cache coherence sensitivity
    HWCC = auto()
    # Track modified tuples
    READ_CXL = "read-cxl"


def main():
    args = cli().parse_args()

    # Requires special handling because we combine multiple input files
    # into a single output figure.
    if (
        args.experiment == Experiment.DEFAULT
        and args.benchmark == common.Benchmark.YCSB
    ):
        return default_ycsb(args)

    input = path(args)
    df = pd.read_csv(input, index_col=0)

    match args.experiment, args.benchmark:
        case Experiment.BASELINE, _:
            baseline(args, df)
        case Experiment.DEFAULT, common.Benchmark.TPCC:
            default_tpcc(args, df)
        case Experiment.HWCC, _:
            hwcc(args, df)
        case Experiment.READ_CXL, _:
            read_cxl(args, df)
        case unknown:
            print(f"Unimplemented combination: {unknown}", file=sys.stderr)
            sys.exit(1)

    plt.savefig(input.with_suffix(f".{args.format}"), dpi=args.dpi, **DEFAULT_SAVE)


def read_cxl(args, df):
    figure, axis = subplots(args, nrows=1, ncols=1)

    for system, row in df.T.loc[["Tigon", "Tigon-ReadCXL"]].iterrows():
        axis.plot(
            df.index,
            row,
            color=color(system),
            marker=marker(system),
            label=system.replace("Tigon", REDACT),
            **DEFAULT_PLOT,
        )

    set_ylim(axis, df)
    figure.legend(**DEFAULT_LEGEND, loc="upper right", borderaxespad=2)


def hwcc(args, df):
    figure, axis = subplots(args, nrows=1, ncols=1)

    for system, row in df.T[::-1].iterrows():
        axis.plot(
            df.index,
            row,
            color=color(system),
            marker=marker(system),
            label=system.replace("Tigon-", ""),
            **DEFAULT_PLOT,
        )

    set_ylim(axis, df)
    figure.legend(**DEFAULT_LEGEND, loc="outside upper center", ncols=5)


def default_tpcc(args, df):
    figure, axis = subplots(args, nrows=1, ncols=1)

    for system, row in df.T.iterrows():
        axis.plot(
            df.index,
            row,
            color=color(system),
            marker=marker(system),
            label=system.replace("Tigon", REDACT),
            **DEFAULT_PLOT,
        )

    # print(
    #     (df.T.loc["Tigon"] - df.T.loc["Sundial-CXL-improved"])
    #     / df.T.loc["Sundial-CXL-improved"]
    #     * 100
    # )

    set_ylim(axis, df)
    figure.legend(**DEFAULT_LEGEND, loc="outside upper center", ncols=len(df))


def default_ycsb(args):
    figure, axes = subplots(args, h=6, nrows=2, ncols=2, sharex=True)
    figure.get_layout_engine().set(hspace=0, h_pad=0.02, wspace=0, w_pad=0.02)

    layout = {
        100: (0, 0),
        95: (0, 1),
        50: (1, 0),
        0: (1, 1),
    }

    for row in range(2):
        axes[row, 0].sharey(axes[row, 1])

    for rw_ratio, (i, j) in layout.items():
        axis = axes[i, j]
        axis.set_title(
            f"{rw_ratio}% R, {100 - rw_ratio}% W",
            y=0.85,
            bbox=dict(boxstyle="round", facecolor="white"),
        )
        args.rw_ratio = rw_ratio
        df = pd.read_csv(path(args), index_col=0)
        df = df.reindex(range(0, 101, 20))

        for system, row in df.T.iterrows():
            axis.plot(
                df.index,
                row,
                color=color(system),
                marker=marker(system),
                label=system.replace("Tigon", REDACT) if i == 0 and j == 0 else None,
                **DEFAULT_PLOT,
            )

        # Overwrite with larger ylim of the two rows
        a = axes[i, 1 - j].get_ylim()[1]
        set_ylim(axis, df)
        b = axes[i, j].get_ylim()[1]
        axis.set_ylim(top=max(a, b))

    figure.legend(
        **DEFAULT_LEGEND,
        # https://github.com/matplotlib/matplotlib/pull/19743
        loc="outside upper center",
        ncol=len(df.T),
    )
    plt.savefig(
        PurePath(args.res_dir, "micro", f"ycsb-0.7.{args.format}"),
        dpi=args.dpi,
        **DEFAULT_SAVE,
    )


def baseline(args, df):
    if args.benchmark == common.Benchmark.YCSB:
        df = df.reindex(list(range(0, 101, 20)))

    figure, axes = subplots(args, nrows=1, ncols=2, sharey=True)
    figure.get_layout_engine().set(wspace=0, w_pad=0.02)

    # Plot Sundial on left
    for system, row in df.T.loc[
        ["Sundial-CXL-improved", "Sundial-CXL", "Sundial-NET"]
    ].iterrows():
        axes[0].plot(
            df.index,
            row,
            color=color(system),
            marker=marker(system),
            label=system.replace("Tigon", REDACT),
            **DEFAULT_PLOT,
        )

    # print(
    #     (df.T.loc["Sundial-CXL-improved"] - df.T.loc["Sundial-NET"])
    #     / df.T.loc["Sundial-NET"]
    #     * 100
    # )

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

    # print(
    #     (df.T.loc["Sundial-CXL-improved"] - df.T.loc["TwoPL-CXL-improved"])
    #     / df.T.loc["Sundial-CXL-improved"]
    #     * 100
    # )

    for axis in axes:
        set_ylim(axis, df)
        axis.tick_params(axis="x", labelsize=8.0)
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
    elif "200MB" in system:
        return "o"
    elif "150MB" in system:
        return "h"
    elif "100MB" in system:
        return "p"
    elif "50MB" in system:
        return "s"
    elif "10MB" in system:
        return "^"
    else:
        return "^"


def subplots(args, w: int = 7, h: int = 3, **kwargs) -> (plt.Figure, plt.Axes):
    figure, axes = plt.subplots(**kwargs)
    figure.set(**DEFAULT_FIGURE)
    figure.set_size_inches(w=w, h=h)
    figure.supxlabel(
        "Multi-partition Transaction Percentage" + " (NewOrder/Payment)"
        if args.benchmark == common.Benchmark.TPCC
        else ""
    )
    figure.supylabel(SUPYLABEL)

    for row in axes if isinstance(axes, Iterable) else [axes]:
        for axis in row if isinstance(row, Iterable) else [row]:
            axis.grid(axis="y")
            axis.grid(axis="x", which="major")
            axis.label_outer(remove_inner_ticks=True)

    return figure, axes


# Must be after plotting
# https://stackoverflow.com/questions/22642511/change-y-range-to-start-from-0-with-matplotlib
def set_ylim(axis, df: pd.DataFrame) -> float:
    max_y = df.to_numpy().max()
    step = 5e5 if max_y > 1e6 else 1e5
    rounded = math.ceil(max_y / step) * step

    axis.set_ylim(0, top=rounded)

    def format(y, pos) -> str:
        if y == 0:
            return "0"

        # HACK: this is about where pyplot switches from 0.5M to 0.25M increments
        if max_y > 2e6:
            return f"{y / 1e6:.1f}M"
        if max_y > 1e6:
            return "{y / 1e6:.2f}M"
        else:
            return f"{int(y / 1e3)}K"

    axis.yaxis.set_major_formatter(ticker.FuncFormatter(format))


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
            parent = "micro" if args.rw_ratio == 0 or args.rw_ratio == 100 else "macro"
            csv = PurePath(parent, f"ycsb-{args.rw_ratio}-{args.zipf_theta}.csv")
        case Experiment.DEFAULT, common.Benchmark.TPCC:
            csv = PurePath("macro", "tpcc.csv")

        case Experiment.HWCC, common.Benchmark.TPCC:
            csv = PurePath("data-movement", "tpcc-data-movement.csv")

        case Experiment.READ_CXL, common.Benchmark.YCSB:
            csv = PurePath(
                "micro", f"ycsb-with-read-cxl-{args.rw_ratio}-{args.zipf_theta}.csv"
            )

    return PurePath(args.res_dir, csv)


def cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="benchmark")

    subparsers.add_parser(common.Benchmark.TPCC)

    ycsb = subparsers.add_parser(common.Benchmark.YCSB)
    ycsb.add_argument("-z", "--zipf-theta", type=float)
    ycsb.add_argument("-r", "--rw-ratio", type=int)

    parser.add_argument("-f", "--format", default="pdf")
    parser.add_argument("-d", "--dpi", type=int)
    parser.add_argument("-e", "--experiment", default=Experiment.DEFAULT)
    parser.add_argument("res_dir")
    return parser


if __name__ == "__main__":
    main()
