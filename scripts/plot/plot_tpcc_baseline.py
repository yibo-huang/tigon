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


if len(sys.argv) != 2:
        print("Usage: " + sys.argv[0] + " res_dir")
        sys.exit(-1)

res_dir = sys.argv[1]

# Read the CSV file into a Pandas DataFrame
res_df = pd.read_csv(res_dir + "/baseline-tpcc.csv")

# Extract the data
x = res_df["Remote_Ratio"]

sundial_cxl_improved_y = res_df["Sundial-CXL-improved"]
sundial_cxl_y = res_df["Sundial-CXL"]
sundial_net_y = res_df["Sundial-NET"]

twopl_cxl_improved_y = res_df["TwoPL-CXL-improved"]
twopl_cxl_y = res_df["TwoPL-CXL"]
twopl_net_y = res_df["TwoPL-NET"]

fig, ax = plt.subplots(nrows=1, ncols=2, figsize=(8, 3.5))

### calculate Y limit for 1 and 2 ###
tmp_list = list()
tmp_list.extend(sundial_cxl_improved_y)
tmp_list.extend(sundial_cxl_y)
tmp_list.extend(sundial_net_y)
tmp_list.extend(twopl_cxl_improved_y)
tmp_list.extend(twopl_cxl_y)
tmp_list.extend(twopl_net_y)

max_y = max(tmp_list)
max_y_rounded_up = math.ceil(max_y / 200000.0) * 200000.0

### subplot 1 ###
ax[0].set_ylim(0, max_y_rounded_up)
ax[0].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'K' if x != 0 else 0))

ax[0].plot(x, sundial_cxl_improved_y, color="#4372c4", marker="s", **DEFAULT_PLOT, label="Sundial+")
ax[0].plot(x, sundial_cxl_y, color="#4372c4", marker="^", **DEFAULT_PLOT, label="Sundial-CXL")
ax[0].plot(x, sundial_net_y, color="#4372c4", marker="o", **DEFAULT_PLOT, label="Sundial-NET")

ax[0].legend(loc='upper right', frameon=True, fancybox=True, framealpha=1, ncol=1)

### subplot 2 ###
ax[1].set_ylim(0, max_y_rounded_up)
ax[1].yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: '{:,.0f}'.format(x/1000) + 'K' if x != 0 else 0))

ax[1].plot(x, twopl_cxl_improved_y, color="#ffc003", marker="s", **DEFAULT_PLOT, label="DS2PL+")
ax[1].plot(x, twopl_cxl_y, color="#ffc003", marker="^", **DEFAULT_PLOT, label="DS2PL-CXL")
ax[1].plot(x, twopl_net_y, color="#ffc003", marker="o", **DEFAULT_PLOT, label="DS2PL-NET")

ax[1].legend(loc='upper right', frameon=True, fancybox=True, framealpha=1, ncol=1)

ax[1].set_yticklabels([])

### global configuration ###
for ax in ax.flat:
    ax.grid(axis='y')

fig = plt.gcf()
fig.tight_layout()
# fig.legend(loc='upper center', bbox_to_anchor=(0.5, 1.04), frameon=False, fancybox=False, framealpha=1, ncol=4)

fig.text(0.5, 0, 'Multi-partition Transaction Percentage (NewOrder/Payment)', ha='center')
fig.text(0, 0.5, 'Throughput (txns/sec)', va='center', rotation='vertical')

plt.savefig(res_dir + "/baseline-tpcc.pdf", format="pdf", bbox_inches="tight")
