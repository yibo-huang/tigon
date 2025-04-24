#!/usr/bin/env python3

import sys
import math
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

### common config START ###

DEFAULT_PLOT = {
    "markersize": 8.0,
    "markeredgewidth": 2.0,
    "markevery": 1,
    "linewidth": 2.0,
}

plt.rcParams["font.size"] = 15

### common config END ###


if len(sys.argv) != 2:
        print("Usage: " + sys.argv[0] + " RESULT_ROOT_DIR")
        sys.exit(-1)

res_root_dir = sys.argv[1]
swcc_budget_res_dir = res_root_dir + "/swcc"

# Read the CSV file into a Pandas DataFrame
res_df_tpcc = pd.read_csv(swcc_budget_res_dir + "/tpcc-swcc.csv")
res_df_ycsb = pd.read_csv(swcc_budget_res_dir + "/ycsb-swcc-95-0.7.csv")

# Extract the data
x_tpcc = res_df_tpcc["Remote_Ratio"]
x_ycsb = res_df_ycsb["Remote_Ratio"]

tigon_y_tpcc = res_df_tpcc["Tigon"]
tigon_no_shared_reader_y_tpcc = res_df_tpcc["Tigon (NoSharedReader)"]
tigon_non_temporal_y_tpcc = res_df_tpcc["Tigon (NonTemporal)"]
tigon_no_swcc_y_tpcc = res_df_tpcc["Tigon (NoSWcc)"]

tigon_y_ycsb = res_df_ycsb["Tigon"]
tigon_no_shared_reader_y_ycsb = res_df_ycsb["Tigon (NoSharedReader)"]
tigon_non_temporal_y_ycsb = res_df_ycsb["Tigon (NonTemporal)"]
tigon_no_swcc_y_ycsb = res_df_ycsb["Tigon (NoSWcc)"]

fig, ax = plt.subplots(nrows=1, ncols=2, figsize=(8, 3), constrained_layout=True)

### subplot 1 ###
tmp_list = list()
tmp_list.extend(tigon_y_tpcc)
tmp_list.extend(tigon_no_shared_reader_y_tpcc)
tmp_list.extend(tigon_non_temporal_y_tpcc)
tmp_list.extend(tigon_no_swcc_y_tpcc)

max_y = max(tmp_list)
max_y_rounded_up_tpcc = math.ceil(max_y / 200000.0) * 200000.0

ax[0].set_ylim(0, max_y_rounded_up_tpcc)
ax[0].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'K' if x != 0 else 0))

ax[0].plot(x_tpcc, tigon_y_tpcc, color="#000000", marker="s", **DEFAULT_PLOT, label="Tigon")
ax[0].plot(x_tpcc, tigon_no_shared_reader_y_tpcc, color="#000000", marker="^", **DEFAULT_PLOT, markerfacecolor = "none", label="NoSharedReader")
ax[0].plot(x_tpcc, tigon_non_temporal_y_tpcc, color="#000000", marker=">", **DEFAULT_PLOT, markerfacecolor = "none", label="NonTemporal")
ax[0].plot(x_tpcc, tigon_no_swcc_y_tpcc, color="#000000", marker="o", **DEFAULT_PLOT, markerfacecolor = "none", label="NoSWcc")

xticks = ax[0].xaxis.get_major_ticks()
xticks[1].label1.set_visible(False)
xticks[3].label1.set_visible(False)
xticks[5].label1.set_visible(False)

### subplot 2 ###
tmp_list = list()
tmp_list.extend(tigon_y_ycsb)
tmp_list.extend(tigon_no_shared_reader_y_ycsb)
tmp_list.extend(tigon_non_temporal_y_ycsb)
tmp_list.extend(tigon_no_swcc_y_ycsb)

max_y = max(tmp_list)
max_y_rounded_up_ycsb = math.ceil(max_y / 200000.0) * 200000.0

ax[1].set_ylim(0, 2000000)
ax[1].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.1f}'.format(x/1000000) + 'M' if x != 0 else 0))

ax[1].plot(x_ycsb, tigon_y_ycsb, color="#000000", marker="s", **DEFAULT_PLOT)
ax[1].plot(x_ycsb, tigon_no_shared_reader_y_ycsb, color="#000000", marker="^", **DEFAULT_PLOT, markerfacecolor = "none")
ax[1].plot(x_ycsb, tigon_non_temporal_y_ycsb, color="#000000", marker=">", **DEFAULT_PLOT, markerfacecolor = "none")
ax[1].plot(x_ycsb, tigon_no_swcc_y_ycsb, color="#000000", marker="o", **DEFAULT_PLOT, markerfacecolor = "none")

ax[1].set_xticks(np.arange(min(x_ycsb), max(x_ycsb)+1, 20.0))

### global configuration ###
for ax in ax.flat:
    ax.grid(axis='y')

fig = plt.gcf()
# fig.tight_layout()
fig.legend(loc='upper center', bbox_to_anchor=(0.5, 1.13), frameon=False, fancybox=False, framealpha=1, ncol=4, columnspacing=1)

fig.text(0.5, -0.06, 'Multi-partition Transaction Percentage', ha='center')
fig.text(0.27, -0.15, '(a) TPC-C', ha='center')
fig.text(0.79, -0.15, '(b) YCSB (95%R, 5%W)', ha='center')
fig.text(-0.03, 0.5, 'Throughput (txns/sec)', va='center', rotation='vertical')

plt.savefig(swcc_budget_res_dir + "/swcc.pdf", format="pdf", bbox_inches="tight")
