#!/usr/bin/env python3

import sys
import math
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

if len(sys.argv) != 4:
        print("Usage: ./plot_ycsb_dynamic_data_movement.py res_dir rw_ratio zipf_theta")
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
res_csv = res_dir + "/ycsb-rmw-dynamic-data-movement-" + rw_ratio + "-" + zipf_theta + ".csv"

# Read the CSV file into a Pandas DataFrame
res_df = pd.read_csv(res_csv)

# Extract the data
x = res_df["Remote_Ratio"]

tigon_lru_1000_wt_y = res_df["Tigon-LRU-1000"]
tigon_lru_10000_wt_y = res_df["Tigon-LRU-10000"]
tigon_lru_100000_wt_y = res_df["Tigon-LRU-100000"]
tigon_lru_1000000_wt_y = res_df["Tigon-LRU-1000000"]
tigon_lru_10000000_wt_y = res_df["Tigon-LRU-10000000"]

plt.xlabel("Multi-host Transaction Percentage", **basic_font)
plt.ylabel("Throughput (txns/sec)", **basic_font)

# Configure axis range
tmp_list = list()
tmp_list.extend(tigon_lru_1000_wt_y)
tmp_list.extend(tigon_lru_10000_wt_y)
tmp_list.extend(tigon_lru_100000_wt_y)
tmp_list.extend(tigon_lru_1000000_wt_y)
tmp_list.extend(tigon_lru_10000000_wt_y)
max_y = max(tmp_list)
max_y_rounded_up = math.ceil(max_y / 200000.0) * 200000.0
plt.ylim(0, max_y_rounded_up)

# Transform Y axises
ax = plt.subplot(111)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'K' if x != 0 else 0))

# Create the line plot
plt.plot(x, tigon_lru_1000_wt_y, color="#62615d", marker="s", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-1000")
plt.plot(x, tigon_lru_10000_wt_y, color="#CD5C5C", marker="s", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-10000")
plt.plot(x, tigon_lru_100000_wt_y, color="#4372c4", marker="s", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-100000")
plt.plot(x, tigon_lru_1000000_wt_y, color="#000000", marker="s", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-1000000")
plt.plot(x, tigon_lru_10000000_wt_y, color="#FFF000", marker="s", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-10000000")

# Configure legend
ax.legend(frameon=False, fancybox=False, framealpha=1, ncol=2, prop={**basic_font})

plt.savefig(res_dir + "ycsb-rmw-dynamic-data-movement-" + rw_ratio + "-" + zipf_theta + ".pdf", format="pdf", bbox_inches="tight")
