#!/usr/bin/env python3

import sys
import math
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

if len(sys.argv) != 4:
        print("Usage: ./plot-ycsb-remote-txn-overhead.py res_dir rw_ratio zipf_theta")
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
res_csv = res_dir + "/ycsb-rmw-remote-txn-overhead-" + rw_ratio + "-" + zipf_theta + ".csv"

# Read the CSV file into a Pandas DataFrame
res_df = pd.read_csv(res_csv)

# Extract the data
x = res_df["Remote_Ratio"]

twoplpasha_cxl_y = res_df["Tigon"]
sundial_cxl_y = res_df["Sundial-CXL"]
sundial_net_y = res_df["Sundial-NET"]
twopl_cxl_y = res_df["TwoPL-CXL"]
twopl_net_y = res_df["TwoPL-NET"]
motor_y = res_df["Motor-RDMA(28thd)"]

plt.xlabel("Multi-host Transaction Percentage", **basic_font)
plt.ylabel("Throughput (txns/sec)", **basic_font)

# Configure axis range
tmp_list = list()
tmp_list.extend(twoplpasha_cxl_y)
tmp_list.extend(sundial_cxl_y)
tmp_list.extend(sundial_net_y)
tmp_list.extend(twopl_cxl_y)
tmp_list.extend(twopl_net_y)
tmp_list.extend(motor_y)
max_y = max(tmp_list)
max_y_rounded_up = math.ceil(max_y / 200000.0) * 200000.0
plt.ylim(0, max_y_rounded_up)

# Transform Y axises
ax = plt.subplot(111)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'K' if x != 0 else 0))

# Create the line plot
plt.plot(x, twoplpasha_cxl_y, color="#000000", marker="o", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="TwoPLPasha-CXL")
plt.plot(x, sundial_cxl_y, color="#ffc003", marker="^", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Sundial-CXL")
plt.plot(x, twopl_cxl_y, color="#62615d", marker=">", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="TwoPL-CXL")
plt.plot(x, sundial_net_y, color="#CD5C5C", marker="s", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Sundial-NET")
plt.plot(x, twopl_net_y, color="#4372c4", marker="s", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="TwoPL-NET")
plt.plot(x, motor_y, color="#3CB371", marker="s", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Motor")

# Configure legend
ax.legend(loc='upper center', frameon=False, fancybox=False, framealpha=1, ncol=2, prop={**basic_font})

plt.savefig(res_dir + "ycsb-rmw-remote-txn-overhead-" + rw_ratio + "-" + zipf_theta + ".pdf", format="pdf", bbox_inches="tight")
