#!/usr/bin/env python3

import sys
import math
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

### common config START ###

DEFAULT_PLOT = {
    "markersize": 10.0,
    "markeredgewidth": 1.2,
    "markerfacecolor": "none",
    "markevery": 1,
    "linewidth": 1.0,
}

plt.rcParams["font.size"] = 11

### common config END ###


if len(sys.argv) != 2:
        print("Usage: " + sys.argv[0] + " res_dir")
        sys.exit(-1)

res_dir = sys.argv[1]

# Read the CSV file into a Pandas DataFrame
res_df_tpcc = pd.read_csv(res_dir + "/tpcc-data-movement.csv")
res_df_ycsb = pd.read_csv(res_dir + "/ycsb-data-movement-95-0.7.csv")

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

fig, ax = plt.subplots(nrows=1, ncols=2, figsize=(9, 4))

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
ax[0].plot(x_tpcc, tigon_150_y_tpcc, color="#000000", marker="^", **DEFAULT_PLOT, label="150MB")
ax[0].plot(x_tpcc, tigon_100_y_tpcc, color="#000000", marker=">", **DEFAULT_PLOT, label="100MB")
ax[0].plot(x_tpcc, tigon_50_y_tpcc, color="#000000", marker="p", **DEFAULT_PLOT, label="50MB")
ax[0].plot(x_tpcc, tigon_10_y_tpcc, color="#000000", marker="o", **DEFAULT_PLOT, label="10MB")

# ax[0].text(0.5, 0.95, 'TPC-C', horizontalalignment='center', verticalalignment='top', transform=ax[0].transAxes, bbox=dict(facecolor='white', edgecolor='black', boxstyle='round'))

ax[0].set_xlabel("Multi-partition Transaction Percentage (NewOrder/Payment)"
                 "\n"
                 "(a) TPC-C")

### subplot 2 ###
tmp_list = list()
tmp_list.extend(tigon_200_y_ycsb)
tmp_list.extend(tigon_150_y_ycsb)
tmp_list.extend(tigon_100_y_ycsb)
tmp_list.extend(tigon_50_y_ycsb)
tmp_list.extend(tigon_10_y_ycsb)

max_y = max(tmp_list)
max_y_rounded_up_ycsb = math.ceil(max_y / 2000000.0) * 2000000.0

ax[1].set_ylim(0, max_y_rounded_up_ycsb)
ax[1].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'K' if x != 0 else 0))

ax[1].plot(x_ycsb, tigon_200_y_ycsb, color="#000000", marker="s", **DEFAULT_PLOT)
ax[1].plot(x_ycsb, tigon_150_y_ycsb, color="#000000", marker="^", **DEFAULT_PLOT)
ax[1].plot(x_ycsb, tigon_100_y_ycsb, color="#000000", marker=">", **DEFAULT_PLOT)
ax[1].plot(x_ycsb, tigon_50_y_ycsb, color="#000000", marker="p", **DEFAULT_PLOT)
ax[1].plot(x_ycsb, tigon_10_y_ycsb, color="#000000", marker="o", **DEFAULT_PLOT)

# ax[1].text(0.5, 0.95, 'YCSB (95%R, 5%W)', horizontalalignment='center', verticalalignment='top', transform=ax[1].transAxes, bbox=dict(facecolor='white', edgecolor='black', boxstyle='round'))

ax[1].set_xlabel("Multi-partition Transaction Percentage"
                 "\n"
                 "(a) YCSB (95%R, 5%W)")

### global configuration ###
for ax in ax.flat:
    ax.grid(axis='y')

fig = plt.gcf()
fig.tight_layout()
fig.legend(loc='upper center', bbox_to_anchor=(0.5, 1.05), frameon=False, fancybox=False, framealpha=1, ncol=5)

# fig.text(0.5, 0, 'Multi-partition Transaction Percentage', ha='center')
fig.text(-0.01, 0.5, 'Throughput (txns/sec)', va='center', rotation='vertical')

plt.savefig(res_dir + "/hwcc.pdf", format="pdf", bbox_inches="tight")
