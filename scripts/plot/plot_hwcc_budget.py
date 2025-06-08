#!/usr/bin/env python3

import sys
import math
import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

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
hwcc_budget_res_dir = res_root_dir + "/hwcc_budget"

# Read the CSV file into a Pandas DataFrame
res_df_tpcc = pd.read_csv(hwcc_budget_res_dir + "/tpcc-hwcc-budget.csv")
res_df_ycsb = pd.read_csv(hwcc_budget_res_dir + "/ycsb-hwcc-budget-95-0.7.csv")

# Extract the data
x_tpcc = res_df_tpcc["Remote_Ratio"]
x_ycsb = res_df_ycsb["Remote_Ratio"]

tigon_200_y_tpcc = res_df_tpcc["Tigon-200MB"]
tigon_150_y_tpcc = res_df_tpcc["Tigon-150MB"]
tigon_100_y_tpcc = res_df_tpcc["Tigon-100MB"]
tigon_50_y_tpcc = res_df_tpcc["Tigon-50MB"]
tigon_10_y_tpcc = res_df_tpcc["Tigon-10MB"]

tigon_200_y_ycsb = res_df_ycsb["Tigon-200MB"]
tigon_150_y_ycsb = res_df_ycsb["Tigon-150MB"]
tigon_100_y_ycsb = res_df_ycsb["Tigon-100MB"]
tigon_50_y_ycsb = res_df_ycsb["Tigon-50MB"]
tigon_10_y_ycsb = res_df_ycsb["Tigon-10MB"]

fig, ax = plt.subplots(nrows=1, ncols=2, figsize=(8, 3), constrained_layout=True)

### subplot 1 ###
tmp_list = list()
tmp_list.extend(tigon_200_y_tpcc)
tmp_list.extend(tigon_150_y_tpcc)
tmp_list.extend(tigon_100_y_tpcc)
tmp_list.extend(tigon_50_y_tpcc)
tmp_list.extend(tigon_10_y_tpcc)

max_y = max(tmp_list)
max_y_rounded_up_tpcc = math.ceil(max_y / 200000.0) * 200000.0

ax[0].set_ylim(0, max_y_rounded_up_tpcc)
ax[0].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'K' if x != 0 else 0))

ax[0].plot(x_tpcc, tigon_200_y_tpcc, color="#000000", marker="s", **DEFAULT_PLOT, label="Tigon (200MB)")
ax[0].plot(x_tpcc, tigon_150_y_tpcc, color="#000000", marker="^", **DEFAULT_PLOT, markerfacecolor = "none", label="150MB")
ax[0].plot(x_tpcc, tigon_100_y_tpcc, color="#000000", marker=">", **DEFAULT_PLOT, markerfacecolor = "none", label="100MB")
ax[0].plot(x_tpcc, tigon_50_y_tpcc, color="#000000", marker="v", **DEFAULT_PLOT, markerfacecolor = "none", label="50MB")
ax[0].plot(x_tpcc, tigon_10_y_tpcc, color="#000000", marker="o", **DEFAULT_PLOT, markerfacecolor = "none", label="10MB")

xticks = ax[0].xaxis.get_major_ticks()
xticks[1].label1.set_visible(False)
xticks[3].label1.set_visible(False)
xticks[5].label1.set_visible(False)

# ax[0].text(0.5, 0.95, 'TPC-C', horizontalalignment='center', verticalalignment='top', transform=ax[0].transAxes, bbox=dict(facecolor='white', edgecolor='black', boxstyle='round'))

### subplot 2 ###
tmp_list = list()
tmp_list.extend(tigon_200_y_ycsb)
tmp_list.extend(tigon_150_y_ycsb)
tmp_list.extend(tigon_100_y_ycsb)
tmp_list.extend(tigon_50_y_ycsb)
tmp_list.extend(tigon_10_y_ycsb)

max_y = max(tmp_list)
max_y_rounded_up_ycsb = math.ceil(max_y / 500000.0) * 500000.0

ax[1].set_ylim(0, max_y_rounded_up_ycsb)
ax[1].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.1f}'.format(x/1000000) + 'M' if x != 0 else 0))

ax[1].plot(x_ycsb, tigon_200_y_ycsb, color="#000000", marker="s", **DEFAULT_PLOT)
ax[1].plot(x_ycsb, tigon_150_y_ycsb, color="#000000", marker="^", markerfacecolor = "none", **DEFAULT_PLOT)
ax[1].plot(x_ycsb, tigon_100_y_ycsb, color="#000000", marker=">", markerfacecolor = "none", **DEFAULT_PLOT)
ax[1].plot(x_ycsb, tigon_50_y_ycsb, color="#000000", marker="v", markerfacecolor = "none", **DEFAULT_PLOT)
ax[1].plot(x_ycsb, tigon_10_y_ycsb, color="#000000", marker="o", markerfacecolor = "none", **DEFAULT_PLOT)

ax[1].set_xticks(np.arange(min(x_ycsb), max(x_ycsb)+1, 20.0))

# ax[1].text(0.5, 0.95, 'YCSB (95%R, 5%W)', horizontalalignment='center', verticalalignment='top', transform=ax[1].transAxes, bbox=dict(facecolor='white', edgecolor='black', boxstyle='round'))

### global configuration ###
for ax in ax.flat:
    ax.grid(axis='y')

fig = plt.gcf()
# fig.tight_layout()
fig.legend(loc='upper center', bbox_to_anchor=(0.5, 1.13), frameon=False, fancybox=False, framealpha=1, ncol=5, columnspacing=0.8)

fig.text(0.5, -0.06, 'Multi-partition Transaction Percentage', ha='center')
fig.text(0.27, -0.15, '(a) TPC-C', ha='center')
fig.text(0.79, -0.15, '(b) YCSB (95%R, 5%W)', ha='center')
fig.text(-0.03, 0.5, 'Throughput (txns/sec)', va='center', rotation='vertical')

plt.savefig(hwcc_budget_res_dir + "/hwcc_budget.pdf", format="pdf", bbox_inches="tight")
