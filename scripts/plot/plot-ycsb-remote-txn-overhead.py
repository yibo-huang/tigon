#!/usr/bin/env python3

import sys
import math
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

if len(sys.argv) != 3:
        print("Usage: ./plot-ycsb-remote-txn-overhead.py res_dir zipf_theta")
        sys.exit(-1)

res_dir = sys.argv[1]
zipf_theta = sys.argv[2]

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
res_csv = res_dir + "/ycsb-" + zipf_theta + ".csv"

# Read the CSV file into a Pandas DataFrame
res_df = pd.read_csv(res_csv)

# Extract the data
x = res_df["Remote_Ratio"]

sundialpasha_cxl_y = res_df["SundialPasha-CXL"]
sundial_cxl_y = res_df["Sundial-CXL"]
# lotus_cxl_y = res_df["Lotus-CXL"]
sundialpasha_net_y = res_df["SundialPasha-NET"]
sundial_net_y = res_df["Sundial-NET"]
# lotus_net_y = res_df["Lotus-NET"]


# plt.title("TPCC Remote Transaction Overhead", **basic_font)
plt.xlabel("Multi-host Transaction Percentage", **basic_font)
plt.ylabel("Throughput (txns/sec)", **basic_font)

# Configure axis range
tmp_list = list()
tmp_list.extend(sundialpasha_cxl_y)
tmp_list.extend(sundial_cxl_y)
tmp_list.extend(sundialpasha_net_y)
tmp_list.extend(sundial_net_y)
max_y = max(tmp_list)
max_y_rounded_up = math.ceil(max_y / 200000.0) * 200000.0
plt.ylim(0, max_y_rounded_up)

# Transform Y axises
ax = plt.subplot(111)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'K' if x != 0 else 0))

# Create the line plot
plt.plot(x, sundialpasha_cxl_y, color="#ed7d31", marker="o", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="SundialPasha-CXL")
plt.plot(x, sundial_cxl_y, color="#ffc003", marker="^", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Sundial-CXL")
# plt.plot(x, lotus_cxl_y, color="#62615d", marker=">", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Lotus-CXL")
plt.plot(x, sundialpasha_net_y, color="#4372c4", marker="s", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="SundialPasha-NET")
plt.plot(x, sundial_net_y, color="#CD5C5C", marker="s", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Sundial-NET")
# plt.plot(x, lotus_net_y, color="#483D8B", marker="s", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Lotus-NET")

# Configure legend
ax.legend(loc='upper center', frameon=False, fancybox=False, framealpha=1, ncol=2, prop={**basic_font})

plt.savefig(res_dir + "ycsb_remote_txn_overhead_" + zipf_theta + ".pdf", format="pdf", bbox_inches="tight")
