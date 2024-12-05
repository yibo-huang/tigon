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
marker_edge_width=1.6
linewidth = 1.0

###  Common configurations for TPCC and YCSB ###
basic_font = {'family' : 'times new roman', 'size' : 12}
plt.xticks(**basic_font)
plt.yticks(**basic_font)

# Configure grid
plt.grid(axis='y')

### plot TPCC ###
res_csv = res_dir + "/tpcc-logging.csv"

# Read the CSV file into a Pandas DataFrame
res_df = pd.read_csv(res_csv)

# Extract the data
x = res_df["Remote_Ratio"]

tigon_no_logging_y = res_df["Tigon-no-logging"]
tigon_1ms_y = res_df["Tigon-1ms"]
tigon_10ms_y = res_df["Tigon-10ms"]
tigon_20ms_y = res_df["Tigon-20ms"]
tigon_30ms_y = res_df["Tigon-30ms"]
tigon_40ms_y = res_df["Tigon-40ms"]
tigon_50ms_y = res_df["Tigon-50ms"]

plt.xlabel("Multi-partition Transaction Percentage", **basic_font)
plt.ylabel("Throughput (txns/sec)", **basic_font)

# Configure axis range
tmp_list = list()
tmp_list.extend(tigon_no_logging_y)
tmp_list.extend(tigon_1ms_y)
tmp_list.extend(tigon_10ms_y)
tmp_list.extend(tigon_20ms_y)
tmp_list.extend(tigon_30ms_y)
tmp_list.extend(tigon_40ms_y)
tmp_list.extend(tigon_50ms_y)

max_y = max(tmp_list)
max_y_rounded_up = math.ceil(max_y / 200000.0) * 200000.0
plt.ylim(0, max_y_rounded_up)

# Transform Y axises
ax = plt.subplot(111)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'K' if x != 0 else 0))

# Create the line plot
plt.plot(x, tigon_no_logging_y, color="#ffc003", marker="^", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-NoLogging")
plt.plot(x, tigon_1ms_y, color="#62615d", marker=">", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-1ms")
plt.plot(x, tigon_10ms_y, color="#BDB76B", marker="^", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-10ms")
plt.plot(x, tigon_20ms_y, color="#8B008B", marker=">", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-20ms")
plt.plot(x, tigon_30ms_y, color="#8B008B", marker=">", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-30ms")
plt.plot(x, tigon_40ms_y, color="#8B008B", marker=">", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-40ms")
plt.plot(x, tigon_50ms_y, color="#8B008B", marker=">", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Tigon-50ms")

# Configure legend
ax.legend(loc='upper center', frameon=False, fancybox=False, framealpha=1, ncol=2, prop={**basic_font})

plt.savefig(res_dir + "/tpcc-logging.pdf", format="pdf", bbox_inches="tight")
