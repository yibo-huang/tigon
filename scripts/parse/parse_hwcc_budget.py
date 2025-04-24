#!/usr/bin/env python3

import sys
import csv
import fileinput
import pandas as pd
import os

from common import get_row, parse_results

def construct_input_list_ycsb_hwcc_budget(hwcc_budget_res_dir, rw_ratio, zipf_theta):
        input_file_list = list()
        input_file_list.append(("Tigon-200MB", hwcc_budget_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-Clock-OnDemand-209715200-1-WriteThrough-None-GROUP_WAL-20000-0.txt"))
        input_file_list.append(("Tigon-150MB", hwcc_budget_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-Clock-OnDemand-157286400-1-WriteThrough-None-GROUP_WAL-20000-0.txt"))
        input_file_list.append(("Tigon-100MB", hwcc_budget_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-Clock-OnDemand-104857600-1-WriteThrough-None-GROUP_WAL-20000-0.txt"))
        input_file_list.append(("Tigon-50MB", hwcc_budget_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-Clock-OnDemand-52428800-1-WriteThrough-None-GROUP_WAL-20000-0.txt"))
        input_file_list.append(("Tigon-10MB", hwcc_budget_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-Clock-OnDemand-10485760-1-WriteThrough-None-GROUP_WAL-20000-0.txt"))
        return input_file_list

def parse_ycsb_hwcc_budget(hwcc_budget_res_dir, rw_ratio, zipf_theta):
        input_file_list = construct_input_list_ycsb_hwcc_budget(hwcc_budget_res_dir, rw_ratio, zipf_theta)
        output_file_name = hwcc_budget_res_dir + "/ycsb-hwcc-budget-" + rw_ratio + "-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_results(input_file_list, output_file_name, header_row)

def construct_input_list_tpcc_hwcc_budget(hwcc_budget_res_dir):
        input_file_list = list()
        input_file_list.append(("Tigon-200MB", hwcc_budget_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-Clock-OnDemand-209715200-1-WriteThrough-None-GROUP_WAL-20000-0.txt"))
        input_file_list.append(("Tigon-150MB", hwcc_budget_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-Clock-OnDemand-157286400-1-WriteThrough-None-GROUP_WAL-20000-0.txt"))
        input_file_list.append(("Tigon-100MB", hwcc_budget_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-Clock-OnDemand-104857600-1-WriteThrough-None-GROUP_WAL-20000-0.txt"))
        input_file_list.append(("Tigon-50MB", hwcc_budget_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-Clock-OnDemand-52428800-1-WriteThrough-None-GROUP_WAL-20000-0.txt"))
        input_file_list.append(("Tigon-10MB", hwcc_budget_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-Clock-OnDemand-10485760-1-WriteThrough-None-GROUP_WAL-20000-0.txt"))
        return input_file_list

def parse_tpcc_hwcc_budget(hwcc_budget_res_dir):
        input_file_list = construct_input_list_tpcc_hwcc_budget(hwcc_budget_res_dir)
        output_file_name = hwcc_budget_res_dir + "/tpcc-hwcc-budget.csv"
        header_row = ["Remote_Ratio", "0/0", "10/15", "20/30", "30/45", "40/60", "50/75", "60/90"]
        parse_results(input_file_list, output_file_name, header_row)


if len(sys.argv) != 2:
        print("Usage: " + sys.argv[0] + " RESULT_ROOT_DIR")
        sys.exit(-1)

res_root_dir = sys.argv[1]
hwcc_budget_res_dir = res_root_dir + "/hwcc_budget"

parse_tpcc_hwcc_budget(hwcc_budget_res_dir)
parse_ycsb_hwcc_budget(hwcc_budget_res_dir, "95", "0.7")
