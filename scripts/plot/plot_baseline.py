#!/usr/bin/env python3

import argparse
import math
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

marker_size = 10.0
marker_edge_width = 1.6
linewidth = 1.0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("res_dir")
    parser.add_argument("-z", "--zipf-theta")
    parser.add_argument("-r", "--rw-ratio")
    args = parser.parse_args()

    csv = None
    benchmark = None
    if args.zipf_theta is not None and args.rw_ratio is not None:
        benchmark = "ycsb"
        csv = os.path.join(
            args.res_dir,
            "micro",
            f"baseline-ycsb-{args.rw_ratio}-{args.zipf_theta}.csv",
        )
    else:
        benchmark = "tpcc"
        csv = os.path.join(args.res_dir, "macro", "baseline-tpcc.csv")

    df = pd.read_csv(csv, index_col=0)

    if benchmark == "ycsb":
        df = df.reindex(list(range(0, 101, 20)))

    xs = df.index

    # Configure axis range
    max_y = df.to_numpy().max()
    max_y_rounded_up = math.ceil(max_y / 200000.0) * 200000.0
    plt.ylim(0, max_y_rounded_up)

    figure, axes = plt.subplots(nrows=1, ncols=2, sharey=True)
    figure.set_size_inches(w=6, h=3)
    figure.set_layout_engine("constrained")
    figure.supxlabel(
        "Multi-partition Transaction Percentage"
        + (" (NewOrder/Payment)" if benchmark == "tpcc" else "")
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
            markersize=marker_size,
            linewidth=linewidth,
            markeredgewidth=marker_edge_width,
            mfc="none",
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
            markersize=marker_size,
            linewidth=linewidth,
            markeredgewidth=marker_edge_width,
            mfc="none",
            label=label,
        )

    for axis in axes:
        axis.tick_params(axis="x", labelsize=8.0)
        axis.grid(axis="y")

        formatter = None
        if max_y > 1e6:
            formatter = ticker.FuncFormatter(
                lambda x, pos: 0 if x == 0 else f"{x / 1e6:.1f}M"
            )
        else:
            formatter = ticker.FuncFormatter(
                lambda x, pos: 0 if x == 0 else f"{int(x / 1e3)}K"
            )

        axis.yaxis.set_major_formatter(formatter)
        axis.legend(
            loc="upper right",
            frameon=False,
            fancybox=False,
            framealpha=1,
            markerfirst=False,
        )

    if benchmark == "tpcc":
        plt.savefig(
            os.path.join(args.res_dir, "macro", "baseline-tpcc.pdf"),
            format="pdf",
            bbox_inches="tight",
        )
    else:
        plt.savefig(
            os.path.join(
                args.res_dir,
                "micro",
                f"baseline-ycsb-{args.rw_ratio}-{args.zipf_theta}.pdf",
            ),
            format="pdf",
            bbox_inches="tight",
        )


if __name__ == "__main__":
    main()
