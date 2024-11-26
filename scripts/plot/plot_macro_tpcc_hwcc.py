#!/usr/bin/env python3

import sys
import math
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

if len(sys.argv) != 2:
    print("Usage: " + sys.argv[0] + " res_dir")
    sys.exit(-1)

res_dir = sys.argv[1] + "/macro"

marker_size = 10.0
marker_edge_width = 1.2
linewidth = 0.8

###  Common configurations for TPCC and YCSB ###
basic_font = {"family": "times new roman", "size": 12}
plt.xticks(**basic_font)
plt.yticks(**basic_font)

# Configure grid
plt.grid(axis="y")

### plot TPCC ###
res_csv = res_dir + "/tpcc-hwcc.csv"

# Read the CSV file into a Pandas DataFrame
res_df = pd.read_csv(res_csv)

# Extract the data
x = res_df["Remote_Ratio"]

hwcc_sizes = [
    0,
    10000000,
    50000000,
    100000000,
    150000000,
    200000000,
]

plt.xlabel("Multi-partition Transaction Percentage", **basic_font)
plt.ylabel("Throughput (txns/sec)", **basic_font)

# Configure axis range
max_y = max([max(res_df[str(size)]) for size in hwcc_sizes])
max_y_rounded_up = math.ceil(max_y / 200000.0) * 200000.0
plt.ylim(0, max_y_rounded_up)

# Transform Y axises
ax = plt.subplot(111)
ax.yaxis.set_major_formatter(
    ticker.FuncFormatter(
        lambda x, pos: "{:,.0f}".format(x / 1000) + "K" if x != 0 else 0
    )
)

# Create the line plot
for size in hwcc_sizes:
    plt.plot(
        x,
        res_df[str(size)],
        marker="o",
        markersize=marker_size,
        linewidth=linewidth,
        markeredgewidth=marker_edge_width,
        mfc="none",
        label=f"Tigon-{int(size / 1000 / 1000) if size > 0 else '∞'}",
    )

# Configure legend
ax.legend(
    loc="upper center",
    frameon=False,
    fancybox=False,
    framealpha=1,
    ncol=2,
    prop={**basic_font},
)

plt.savefig(res_dir + "/tpcc-hwcc.pdf", format="pdf", bbox_inches="tight")
