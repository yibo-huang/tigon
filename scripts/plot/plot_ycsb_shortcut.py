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
basic_font = {'family' : 'arial', 'size' : 12}
plt.xticks(**basic_font)
plt.yticks(**basic_font)

# Configure grid
plt.grid(axis='y')

### plot TPCC ###
res_csv = res_dir + "/ycsb-shortcut-" + rw_ratio + "-" + zipf_theta + ".csv"

# Read the CSV file into a Pandas DataFrame
res_df = pd.read_csv(res_csv)

# Extract the data
x = res_df["Remote_Ratio"]

tigon_y = res_df["Tigon"]
tigon_no_shortcut_y = res_df["Tigon-AlwaysSearchCXL"]

plt.xlabel("Multi-partition Transaction Percentage", **basic_font)
plt.ylabel("Throughput (txns/sec)", **basic_font)

# Configure axis range
tmp_list = list()
tmp_list.extend(tigon_y)
tmp_list.extend(tigon_no_shortcut_y)

max_y = max(tmp_list)
max_y_rounded_up = math.ceil(max_y / 200000.0) * 200000.0
plt.ylim(0, max_y_rounded_up)

# Transform Y axises
ax = plt.subplot(111)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'K' if x != 0 else 0))

# Create the line plot
plt.plot(x, tigon_y, color="#ffc003", marker="o", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon")
plt.plot(x, tigon_no_shortcut_y, color="#BDB76B", marker="^", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-AlwaysSearchCXL")

# Configure legend
ax.legend(loc='upper center', frameon=False, fancybox=False, framealpha=1, ncol=2, prop={**basic_font})

plt.savefig(res_dir + "/ycsb-shortcut-" + rw_ratio + "-" + zipf_theta + ".pdf", format="pdf", bbox_inches="tight")
