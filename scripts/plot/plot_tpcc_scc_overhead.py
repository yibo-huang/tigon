#!/usr/bin/env python3

import sys
import math
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

if len(sys.argv) != 2:
        print("Usage: ./plot_tpcc_scc_overhead.py res_dir")
        sys.exit(-1)

res_dir = sys.argv[1]

marker_size = 10.0
marker_edge_width=1.6
linewidth = 1.0

###  Common configurations for TPCC and YCSB ###
basic_font = {'family' : 'times new roman', 'size' : 12}
plt.xticks(**basic_font)
plt.yticks(**basic_font)

# Configure grid
plt.grid(axis='y')

### plot TPCC ###
res_csv = res_dir + "/tpcc-scc-overhead.csv"

# Read the CSV file into a Pandas DataFrame
res_df = pd.read_csv(res_csv)

# Extract the data
x = res_df["Remote_Ratio"]

twoplpasha_cxl_wt_y = res_df["Tigon-WT"]
twoplpasha_cxl_hcc_y = res_df["Tigon-HCC"]
twoplpasha_cxl_nt_y = res_df["Tigon-NT"]

plt.xlabel("Multi-host Transaction Percentage (NewOrder/Payment)", **basic_font)
plt.ylabel("Throughput (txns/sec)", **basic_font)

# Configure axis range
tmp_list = list()
tmp_list.extend(twoplpasha_cxl_wt_y)
tmp_list.extend(twoplpasha_cxl_hcc_y)
tmp_list.extend(twoplpasha_cxl_nt_y)

max_y = max(tmp_list)
max_y_rounded_up = math.ceil(max_y / 200000.0) * 200000.0
plt.ylim(0, max_y_rounded_up)

# Transform Y axises
ax = plt.subplot(111)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'K' if x != 0 else 0))

# Create the line plot
plt.plot(x, twoplpasha_cxl_hcc_y, color="#62615d", marker="s", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-HCC")
plt.plot(x, twoplpasha_cxl_wt_y, color="#CD5C5C", marker="s", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-WT")
plt.plot(x, twoplpasha_cxl_nt_y, color="#4372c4", marker="s", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-NT")

# Configure legend
ax.legend(frameon=False, fancybox=False, framealpha=1, ncol=2, prop={**basic_font})

plt.savefig(res_dir + "tpcc-scc-overhead.pdf", format="pdf", bbox_inches="tight")
