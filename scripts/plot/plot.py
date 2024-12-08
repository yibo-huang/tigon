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
    "markeredgewidth": 1.2,
    "markerfacecolor": "none",
    "linewidth": 1.0,
}
DEFAULT_LEGEND = {
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
    # Cache hwcc-pointer
    SEARCH_CXL = "search-cxl"
    # Logging overhead
    LOGGING = auto()
    # SCC policies
    SWCC = auto()

    def name(self) -> str:
        match self:
            case Experiment.HWCC:
                return "HWcc budget"
            case Experiment.READ_CXL:
                return "Track modified tuples"
            case Experiment.SEARCH_CXL:
                return "Cache HWcc-pointer"
            case Experiment.LOGGING:
                return "Logging group commit"
            case Experiment.SWCC:
                return "SWcc policy"


def main():
    args = cli().parse_args()

    if args.benchmark == "all":
        # Ignored by TPC-C, and YCSB workloads are currently run with 0.7
        args.zipf_theta = 0.7

        both = [common.Benchmark.TPCC, common.Benchmark.YCSB]

        # Default TPC-C and YCSB
        for benchmark in both:
            args.benchmark = benchmark
            args.experiment = Experiment.DEFAULT
            plot(args)

        # FIXME: different `res_dir` directory
        # Baseline TPC-C
        # args.benchmark = common.Benchmark.TPCC
        # args.experiment = Experiment.BASELINE
        # plot(args)

        # HWCC TPC-C
        args.benchmark = common.Benchmark.TPCC
        args.experiment = Experiment.HWCC
        plot(args)

        # ReadCXL optimization
        args.benchmark = common.Benchmark.YCSB
        args.experiment = Experiment.READ_CXL
        args.rw_ratio = 100
        plot(args)

        # SearchCXL optimization
        args.benchmark = common.Benchmark.TPCC
        args.experiment = Experiment.SEARCH_CXL
        plot(args)

        args.benchmark = common.Benchmark.YCSB
        args.experiment = Experiment.SEARCH_CXL
        args.rw_ratio = 95
        plot(args)

    else:
        # args.benchmark = common.Benchmark.TPCC
        # args.experiment = Experiment.BASELINE
        plot(args)


def plot(args):
    # Requires special handling because we combine multiple input files
    # into a single output figure.
    if (
        args.experiment == Experiment.DEFAULT
        and args.benchmark == common.Benchmark.YCSB
    ):
        return default_ycsb(args)
    elif args.benchmark == "sensitivity":
        figure, axes = subplots(args, w=16, h=10, nrows=2, ncols=5)

        for col in range(1, 5):
            axes[0, col].sharey(axes[0, 0])
            # axes[0, col].label_outer(remove_inner_ticks=True)

        for col, experiment in enumerate(
            [
                Experiment.SWCC,
                Experiment.HWCC,
                Experiment.LOGGING,
                None,
                Experiment.SEARCH_CXL,
            ]
        ):
            if experiment is None:
                figure.delaxes(axes[0, col])
                continue

            args.benchmark = common.Benchmark.TPCC
            args.experiment = experiment
            plot_one(args, figure, axes[0, col], solo=False)

            axes[0, col].set_title(f"{experiment.name()}\n\nTPC-C")
            axes[0, col].legend(**DEFAULT_LEGEND, loc="lower left")

        for col, (experiment, rw_ratio) in enumerate(
            [
                (Experiment.SWCC, 95),
                (Experiment.HWCC, 95),
                (Experiment.LOGGING, 50),
                (Experiment.READ_CXL, 100),
                (Experiment.SEARCH_CXL, 95),
            ]
        ):
            args.benchmark = common.Benchmark.YCSB
            args.experiment = experiment
            args.zipf_theta = 0.7
            args.rw_ratio = rw_ratio
            plot_one(args, figure, axes[1, col], solo=False)

            axes[1, col].set_title(f"YCSB {rw_ratio}% R")

            # Hack: no TPC-C data yet
            if experiment == Experiment.READ_CXL:
                axes[1, col].set_title(f"{experiment.name()}\n\nYCSB {rw_ratio}% R")
                axes[1, col].legend(**DEFAULT_LEGEND)

            elif experiment == Experiment.SWCC:
                axes[1, col].set_xlabel("HWcc budget (MB)")

        plt.savefig(PurePath(args.res_dir, "sensitivity.pdf"))
        return

    figure, axis = subplots(args, nrows=1, ncols=1)

    output = plot_one(args, figure, axis, True)
    print(f"Saving {args} to {output}...", file=sys.stderr)
    plt.savefig(output, dpi=args.dpi, **DEFAULT_SAVE)


def plot_one(args, figure, axis, solo: bool) -> str:
    input = path(args)
    df = pd.read_csv(input, index_col=0)
    legend = dict()

    def rename(system: str):
        if system == "Tigon-40ms":
            return "Tigon (40ms)"
        elif system == "Tigon-200MB":
            return "Tigon (200MB)"
        return system.replace("Tigon-", "")

    match args.experiment, args.benchmark:
        case Experiment.BASELINE, _:
            baseline(args, df)
        case Experiment.DEFAULT, common.Benchmark.TPCC:
            legend = dict(framealpha=0, loc="outside upper center", ncols=len(df))
        case Experiment.HWCC, _:
            df.columns = df.columns.map(rename)
            legend = dict(loc="outside upper center", ncols=len(df))
        case Experiment.READ_CXL, _:
            df = df.T.loc[["Tigon", "Tigon-ReadCXL"]].T
            df.columns = df.columns.map(rename)
            legend = dict(borderaxespad=2)
        case Experiment.SEARCH_CXL, _:
            df.columns = df.columns.map(rename)
            legend = dict(borderaxespad=2)
        case Experiment.LOGGING, _:
            df = df.T.loc[
                [
                    "Tigon-50ms",
                    "Tigon-40ms",
                    "Tigon-20ms",
                    "Tigon-10ms",
                    "Tigon-1ms",
                    "Tigon-no-logging",
                ]
            ].T
            df.columns = df.columns.map(rename)
            legend = dict(borderaxespad=2)
        case Experiment.SWCC, _:
            df.columns = df.columns.map(
                lambda system: "Tigon (WriteThrough)"
                if system == "Tigon"
                else rename(system)
            )
            df.index = df.index[::-1].map(lambda x: int(x.rstrip("MB")))
            legend = dict(borderaxespad=2)
        case unknown:
            print(f"Unimplemented combination: {unknown}", file=sys.stderr)
            sys.exit(1)

    for system, row in df.T.iterrows():
        axis.plot(
            df.index,
            row,
            color=color(system),
            marker=marker(system),
            label=system.replace("Tigon", REDACT),
            **DEFAULT_PLOT,
        )

    set_ylim(axis, df)
    if solo:
        figure.legend(**DEFAULT_LEGEND, **legend)
    return input.with_suffix(f".{args.format}")


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
        loc="outside upper right",
        ncol=len(df.T),
    )

    output = PurePath(args.res_dir, "micro", f"ycsb-0.7.{args.format}")
    print(f"Saving {args} to {output}...", file=sys.stderr)
    plt.savefig(
        output,
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
        axis.legend(framealpha=1, **DEFAULT_LEGEND)


def color(system: str) -> str:
    if "Sundial" in system:
        return "#62615d"
    elif "TwoPL" in system:
        return "#ffc003"
    elif "Motor" in system:
        return "#ed7d31"
    elif "Tigon" or "MB" in system:
        return "#4372c4"


def marker(system: str) -> str:
    # override
    if "TwoPL-CXL-improved" in system:
        return "^"
    elif "Sundial-CXL-improved" in system:
        return ">"
    elif "Motor" in system:
        return "o"

    if "AlwaysSearchCXL" in system:
        return "o"
    elif "ReadCXL" in system:
        return "o"
    if "NET" in system:
        return "o"
    elif "CXL-improved" in system:
        return "^"
    elif "CXL" in system:
        return "s"
    elif "Phantom" in system:
        return "o"
    elif "200MB" in system:
        return "^"
    elif "150MB" in system:
        return "s"
    elif "100MB" in system:
        return "p"
    elif "50MB" in system:
        return "h"
    elif "10MB" in system:
        return "o"

    elif "50ms" in system:
        return "|"
    elif "40ms" in system:
        return "^"
    elif "20ms" in system:
        return "s"
    elif "10ms" in system:
        return "p"
    elif "1ms" in system:
        return "h"
    elif "no-logging" in system:
        return "o"

    elif "AlwaysMemcpy" in system:
        return "s"
    elif "NoSharedReader" in system:
        return "p"
    elif "NonTemporal" in system:
        return "h"
    elif "NoSCC" in system:
        return "o"

    else:
        return "s"


def subplots(args, w: int = 7, h: int = 3, **kwargs) -> (plt.Figure, plt.Axes):
    figure, axes = plt.subplots(**kwargs)
    figure.set(**DEFAULT_FIGURE)
    if args.benchmark == common.Benchmark.TPCC and args.experiment == Experiment.DEFAULT:
        figure.set_size_inches(w=6, h=4)
    else:
        figure.set_size_inches(w=w, h=h)
    figure.supxlabel(
        "Multi-partition Transaction Percentage"
        + (" (NewOrder/Payment)" if args.benchmark == common.Benchmark.TPCC else ""),
        x=0.55  # make it centered
    )
    figure.supylabel(SUPYLABEL)

    for row in axes if isinstance(axes, Iterable) else [axes]:
        for axis in row if isinstance(row, Iterable) else [row]:
            axis.grid(axis="y")
            axis.grid(axis="x", which="major")
            # axis.label_outer(remove_inner_ticks=True)

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
            return f"{y / 1e6:.2f}M"
        else:
            return f"{int(y / 1e3)}K"

    axis.yaxis.set_major_formatter(ticker.FuncFormatter(format))


def path(args) -> PurePath:
    parent = None
    prefix = ""
    suffix = None

    match args.experiment:
        case Experiment.DEFAULT:
            pass
        case Experiment.BASELINE:
            prefix = "baseline-"
        case Experiment.CUSTOM:
            raise ValueError("unimplemented custom experiment")
        case Experiment.READ_CXL:
            suffix = "-with-read-cxl"
        case Experiment.HWCC:
            parent = "data-movement"
        case Experiment.SEARCH_CXL:
            parent = "shortcut"
        case Experiment.LOGGING:
            parent = "logging"
        case Experiment.SWCC:
            parent = "scc"

    if suffix is None:
        suffix = "" if parent is None else f"-{parent}"

    name = None
    match args.benchmark:
        case common.Benchmark.TPCC:
            parent = "macro" if parent is None else parent
            name = f"{prefix}tpcc{suffix}.csv"
        case common.Benchmark.YCSB:
            parent = "micro" if parent is None else parent
            name = f"{prefix}ycsb{suffix}-{args.rw_ratio}-{args.zipf_theta}.csv"

    return PurePath(args.res_dir, parent, name)


def cli() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="benchmark")

    subparsers.add_parser("all")
    subparsers.add_parser("sensitivity")

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
