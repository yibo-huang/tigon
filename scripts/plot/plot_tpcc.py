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

marker_size = 12.0
marker_edge_width=1.6
linewidth = 1.6

###  Common configurations for TPCC and YCSB ###
basic_font = {'size' : 14}
plt.xticks(**basic_font)
plt.yticks(**basic_font)

# Configure grid
plt.grid(axis='y')

### plot TPCC ###
res_csv = res_dir + "/tpcc.csv"

# Read the CSV file into a Pandas DataFrame
res_df = pd.read_csv(res_csv)

# Extract the data
x = res_df["Remote_Ratio"]

tigon_y = res_df["Tigon"]
sundial_cxl_improved_y = res_df["Sundial-CXL-improved"]
twopl_cxl_improved_y = res_df["TwoPL-CXL-improved"]
motor_y = res_df["Motor"]

plt.xlabel("Multi-partition Transaction Percentage", **basic_font)
plt.ylabel("Throughput (txns/sec)", **basic_font)

# Configure axis range
tmp_list = list()
tmp_list.extend(tigon_y)
tmp_list.extend(sundial_cxl_improved_y)
tmp_list.extend(twopl_cxl_improved_y)
tmp_list.extend(motor_y)

max_y = max(tmp_list)
max_y_rounded_up = math.ceil(max_y / 200000.0) * 200000.0
plt.ylim(0, max_y_rounded_up)

# Transform Y axises
ax = plt.subplot(111)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'K' if x != 0 else 0))

# Create the line plot
plt.plot(x, tigon_y, color="#000000", marker="s", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon")
plt.plot(x, sundial_cxl_improved_y, color="#4372c4", marker="^", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Sundial+")
plt.plot(x, twopl_cxl_improved_y, color="#ffc003", marker=">", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="TwoPL+")
plt.plot(x, motor_y, color="#ed7d31", marker="o", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Motor")

# Configure legend
ax.legend(loc='upper right', bbox_to_anchor=(1.0, 1.02), frameon=False, fancybox=False, framealpha=1, ncol=2, prop={**basic_font})

plt.savefig(res_dir + "/tpcc.pdf", format="pdf", bbox_inches="tight")
