#!/usr/bin/env python3

import sys
import math
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

if len(sys.argv) != 3:
        print("Usage: ./plot_tpcc_remote_txn_overhead.py res_dir scc_mechanism")
        sys.exit(-1)

res_dir = sys.argv[1]
scc_mechanism = sys.argv[2]

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
res_csv = res_dir + "/tpcc-" + scc_mechanism + ".csv"

# Read the CSV file into a Pandas DataFrame
res_df = pd.read_csv(res_csv)

# Extract the data
x = res_df["Remote_Ratio"]

sundialpasha_cxl_y = res_df["SundialPasha-CXL"]
sundial_cxl_y = res_df["Sundial-CXL"]
sundial_net_y = res_df["Sundial-NET"]
twopl_cxl_y = res_df["TwoPL-CXL"]
twopl_net_y = res_df["TwoPL-NET"]

plt.xlabel("Multi-host Transaction Percentage (NewOrder/Payment)", **basic_font)
plt.ylabel("Throughput (txns/sec)", **basic_font)

# Configure axis range
tmp_list = list()
tmp_list.extend(sundialpasha_cxl_y)
tmp_list.extend(sundial_cxl_y)
tmp_list.extend(sundial_net_y)
tmp_list.extend(twopl_cxl_y)
tmp_list.extend(twopl_net_y)
max_y = max(tmp_list)
max_y_rounded_up = math.ceil(max_y / 200000.0) * 200000.0
plt.ylim(0, max_y_rounded_up)

# Transform Y axises
ax = plt.subplot(111)
ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'K' if x != 0 else 0))

# Create the line plot
plt.plot(x, sundialpasha_cxl_y, color="#ed7d31", marker="o", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="SundialPasha-CXL")
plt.plot(x, sundial_cxl_y, color="#ffc003", marker="^", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Sundial-CXL")
plt.plot(x, twopl_cxl_y, color="#62615d", marker="s", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="TwoPL-CXL")
plt.plot(x, sundial_net_y, color="#CD5C5C", marker="s", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="Sundial-NET")
plt.plot(x, twopl_net_y, color="#4372c4", marker="s", markersize=marker_size, linewidth=linewidth, markeredgewidth=marker_edge_width, mfc='none', label="TwoPL-NET")

# Configure legend
ax.legend(loc='upper center', frameon=False, fancybox=False, framealpha=1, ncol=2, prop={**basic_font})

plt.savefig(res_dir + "tpcc-remote-txn-overhead-" + scc_mechanism + ".pdf", format="pdf", bbox_inches="tight")
