#!/usr/bin/env python3

import sys
import math
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

if len(sys.argv) != 4:
        print("Usage: " + sys.argv[0] + " res_dir rw_ratio zipf_theta")
        sys.exit(-1)

res_dir = sys.argv[1]
rw_ratio = sys.argv[2]
zipf_theta = sys.argv[3]

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
res_csv = res_dir + "/ycsb-data-movement-" + rw_ratio + "-" + zipf_theta + ".csv"

# Read the CSV file into a Pandas DataFrame
res_df = pd.read_csv(res_csv)

# Extract the data
x = res_df["Remote_Ratio"]

tigon_200_y = res_df["Tigon-200MB"]
tigon_150_y = res_df["Tigon-150MB"]
tigon_100_y = res_df["Tigon-100MB"]
tigon_50_y = res_df["Tigon-50MB"]
tigon_10_y = res_df["Tigon-10MB"]

plt.xlabel("Multi-partition Transaction Percentage", **basic_font)
plt.ylabel("Throughput (txns/sec)", **basic_font)

# Configure axis range
tmp_list = list()
tmp_list.extend(tigon_200_y)
tmp_list.extend(tigon_150_y)
tmp_list.extend(tigon_100_y)
tmp_list.extend(tigon_50_y)
tmp_list.extend(tigon_10_y)

max_y = max(tmp_list)
max_y_rounded_up = math.ceil(max_y / 200000.0) * 200000.0
plt.ylim(0, max_y_rounded_up)

# Transform Y axises
ax = plt.subplot(111)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'K' if x != 0 else 0))

# Create the line plot
plt.plot(x, tigon_200_y, color="#000000", marker="o", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-200MB")
plt.plot(x, tigon_150_y, color="#62615d", marker=">", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-150MB")
plt.plot(x, tigon_100_y, color="#BDB76B", marker="^", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-100MB")
plt.plot(x, tigon_50_y, color="#ffc003", marker="^", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-50MB")
plt.plot(x, tigon_10_y, color="#8B008B", marker=">", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-10MB")

# Configure legend
ax.legend(loc='upper center', frameon=False, fancybox=False, framealpha=1, ncol=2, prop={**basic_font})

plt.savefig(res_dir + "/ycsb-data-movement-" + rw_ratio + "-" + zipf_theta + ".pdf", format="pdf", bbox_inches="tight")
