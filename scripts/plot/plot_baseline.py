#!/usr/bin/env python3

import sys
import math
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

if len(sys.argv) != 2:
    print("Usage: " + sys.argv[0] + " res_dir")
    sys.exit(-1)

res_dir = sys.argv[1]

marker_size = 10.0
marker_edge_width = 1.6
linewidth = 1.0

### plot TPCC ###
res_csv = res_dir + "/baseline-tpcc.csv"

# Read the CSV file into a Pandas DataFrame
res_df = pd.read_csv(res_csv, index_col=0)

# Extract the data
xs = res_df.index

# Configure axis range
max_y = res_df.to_numpy().max()
max_y_rounded_up = math.ceil(max_y / 200000.0) * 200000.0
plt.ylim(0, max_y_rounded_up)

figure, axes = plt.subplots(nrows=1, ncols=2, sharey=True)
figure.set_size_inches(w=6, h=3)
figure.set_layout_engine("constrained")
figure.supxlabel("Multi-partition Transaction Percentage (NewOrder/Payment)")
figure.supylabel("Throughput (txns/sec)")

for (label, row), marker in zip(
    res_df.T.loc[["Sundial-CXL-improved", "Sundial-CXL", "Sundial-NET"]].iterrows(),
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

for (label, row), marker in zip(
    res_df.T.loc[["TwoPL-CXL-improved", "TwoPL-CXL", "TwoPL-NET"]].iterrows(),
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
    axis.yaxis.set_major_formatter(
        ticker.FuncFormatter(
            lambda x, pos: "{:,.0f}".format(x / 1000) + "K" if x != 0 else 0
        )
    )
    axis.legend(
        loc="upper right",
        frameon=False,
        fancybox=False,
        framealpha=1,
        markerfirst=False,
    )


plt.savefig(res_dir + "/baseline-tpcc.pdf", format="pdf", bbox_inches="tight")
