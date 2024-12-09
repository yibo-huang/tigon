#!/usr/bin/env python3

import sys
import math
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

### common config START ###

DEFAULT_PLOT = {
    "markersize": 6.0,
    "markeredgewidth": 1.2,
    "markerfacecolor": "none",
    "markevery": 1,
    "linewidth": 1.0,
}

plt.rcParams["font.size"] = 11

### common config END ###


if len(sys.argv) != 6:
        print("Usage: " + sys.argv[0] + " cvs_1 csv_2 csv_3 csv_4 output_dir")
        sys.exit(-1)

cvs_1 = sys.argv[1]
cvs_2 = sys.argv[2]
cvs_3 = sys.argv[3]
cvs_4 = sys.argv[4]
output_dir = sys.argv[5]

# Read the CSV file into a Pandas DataFrame
res_df_1 = pd.read_csv(cvs_1)
res_df_2 = pd.read_csv(cvs_2)
res_df_3 = pd.read_csv(cvs_3)
res_df_4 = pd.read_csv(cvs_4)

# Extract the data
x = res_df_1["Remote_Ratio"]

tigon_y_1 = res_df_1["Tigon"]
sundial_cxl_improved_y_1 = res_df_1["Sundial-CXL-improved"]
twopl_cxl_improved_y_1 = res_df_1["TwoPL-CXL-improved"]
motor_y_1 = res_df_1["Motor"]

tigon_y_2 = res_df_2["Tigon"]
sundial_cxl_improved_y_2 = res_df_2["Sundial-CXL-improved"]
twopl_cxl_improved_y_2 = res_df_2["TwoPL-CXL-improved"]
motor_y_2 = res_df_2["Motor"]

tigon_y_3 = res_df_3["Tigon"]
sundial_cxl_improved_y_3 = res_df_3["Sundial-CXL-improved"]
twopl_cxl_improved_y_3 = res_df_3["TwoPL-CXL-improved"]
motor_y_3 = res_df_3["Motor"]

tigon_y_4 = res_df_4["Tigon"]
sundial_cxl_improved_y_4 = res_df_4["Sundial-CXL-improved"]
twopl_cxl_improved_y_4 = res_df_4["TwoPL-CXL-improved"]
motor_y_4 = res_df_4["Motor"]

fig, ax = plt.subplots(nrows=2, ncols=2)

### calculate Y limit for 1 and 2 ###
tmp_list = list()
tmp_list.extend(tigon_y_1)
tmp_list.extend(sundial_cxl_improved_y_1)
tmp_list.extend(twopl_cxl_improved_y_1)
tmp_list.extend(motor_y_1)
tmp_list.extend(tigon_y_2)
tmp_list.extend(sundial_cxl_improved_y_2)
tmp_list.extend(twopl_cxl_improved_y_2)
tmp_list.extend(motor_y_2)

max_y_1_2 = max(tmp_list)
max_y_rounded_up_1_2 = math.ceil(max_y_1_2 / 200000.0) * 200000.0

### calculate Y limit for 3 and 4 ###
tmp_list = list()
tmp_list.extend(tigon_y_3)
tmp_list.extend(sundial_cxl_improved_y_3)
tmp_list.extend(twopl_cxl_improved_y_3)
tmp_list.extend(motor_y_3)
tmp_list.extend(tigon_y_4)
tmp_list.extend(sundial_cxl_improved_y_4)
tmp_list.extend(twopl_cxl_improved_y_4)
tmp_list.extend(motor_y_4)

max_y_3_4 = max(tmp_list)
max_y_rounded_up_3_4 = math.ceil(max_y_3_4 / 200000.0) * 200000.0

### subplot 1 ###
ax[0, 0].set_ylim(0, max_y_rounded_up_1_2)
ax[0, 0].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'K' if x != 0 else 0))

ax[0, 0].plot(x, tigon_y_1, color="#000000", marker="s", **DEFAULT_PLOT, label="Tigon")
ax[0, 0].plot(x, sundial_cxl_improved_y_1, color="#4372c4", marker="^", **DEFAULT_PLOT, label="Sundial+")
ax[0, 0].plot(x, twopl_cxl_improved_y_1, color="#ffc003", marker=">", **DEFAULT_PLOT, label="TwoPL+")
ax[0, 0].plot(x, motor_y_1, color="#ed7d31", marker="o", **DEFAULT_PLOT, label="Motor")

ax[0, 0].set_xticklabels([])

ax[0, 0].text(0.5, 0.95, '100% R, 0%W', horizontalalignment='center', verticalalignment='top', transform=ax[0, 0].transAxes, bbox=dict(facecolor='white', edgecolor='black', boxstyle='round'))

### subplot 2 ###
ax[0, 1].set_ylim(0, max_y_rounded_up_1_2)
ax[0, 1].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'K' if x != 0 else 0))

ax[0, 1].plot(x, tigon_y_2, color="#000000", marker="s", **DEFAULT_PLOT)
ax[0, 1].plot(x, sundial_cxl_improved_y_2, color="#4372c4", marker="^", **DEFAULT_PLOT)
ax[0, 1].plot(x, twopl_cxl_improved_y_2, color="#ffc003", marker=">", **DEFAULT_PLOT)
ax[0, 1].plot(x, motor_y_2, color="#ed7d31", marker="o", **DEFAULT_PLOT)

ax[0, 1].set_xticklabels([])
ax[0, 1].set_yticklabels([])

ax[0, 1].text(0.5, 0.95, '95% R, 5%W', horizontalalignment='center', verticalalignment='top', transform=ax[0, 1].transAxes, bbox=dict(facecolor='white', edgecolor='black', boxstyle='round'))

### subplot 3 ###
ax[1, 0].set_ylim(0, max_y_rounded_up_3_4)
ax[1, 0].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'K' if x != 0 else 0))

ax[1, 0].plot(x, tigon_y_3, color="#000000", marker="s", **DEFAULT_PLOT)
ax[1, 0].plot(x, sundial_cxl_improved_y_3, color="#4372c4", marker="^", **DEFAULT_PLOT)
ax[1, 0].plot(x, twopl_cxl_improved_y_3, color="#ffc003", marker=">", **DEFAULT_PLOT)
ax[1, 0].plot(x, motor_y_3, color="#ed7d31", marker="o", **DEFAULT_PLOT)

ax[1, 0].text(0.5, 0.95, '50% R, 50%W', horizontalalignment='center', verticalalignment='top', transform=ax[1, 0].transAxes, bbox=dict(facecolor='white', edgecolor='black', boxstyle='round'))

### subplot 4 ###
ax[1, 1].set_ylim(0, max_y_rounded_up_3_4)
ax[1, 1].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'K' if x != 0 else 0))

ax[1, 1].plot(x, tigon_y_4, color="#000000", marker="s", **DEFAULT_PLOT)
ax[1, 1].plot(x, sundial_cxl_improved_y_4, color="#4372c4", marker="^", **DEFAULT_PLOT)
ax[1, 1].plot(x, twopl_cxl_improved_y_4, color="#ffc003", marker=">", **DEFAULT_PLOT)
ax[1, 1].plot(x, motor_y_4, color="#ed7d31", marker="o", **DEFAULT_PLOT)

ax[1, 1].text(0.5, 0.95, '0% R, 100%W', horizontalalignment='center', verticalalignment='top', transform=ax[1, 1].transAxes, bbox=dict(facecolor='white', edgecolor='black', boxstyle='round'))

ax[1, 1].set_yticklabels([])

### global configuration ###
for ax in ax.flat:
    ax.grid(axis='y')

fig = plt.gcf()
fig.tight_layout()
fig.legend(loc='upper center', bbox_to_anchor=(0.5, 1.04), frameon=False, fancybox=False, framealpha=1, ncol=4)

fig.text(0.5, 0, 'Multi-partition Transaction Percentage', ha='center')
fig.text(0, 0.5, 'Throughput (txns/sec)', va='center', rotation='vertical')

plt.savefig(output_dir + "/ycsb.pdf", format="pdf", bbox_inches="tight")
