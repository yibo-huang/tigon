#!/usr/bin/env python3

import sys
import csv
import fileinput
import pandas as pd
import os

from common import get_row, parse_results, append_motor_numbers

### Tigon and baselines ###
def construct_input_list_ycsb(ycsb_res_dir, rw_ratio, zipf_theta):
        input_file_list = list()
        input_file_list.append(("Tigon", ycsb_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-Clock-OnDemand-209715200-1-WriteThrough-NonPart-GROUP_WAL-20000-0.txt"))

        input_file_list.append(("Sundial-CXL-improved", ycsb_res_dir + "/ycsb-Sundial-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-NoMoveOut-OnDemand-0-0-NoOP-None-GROUP_WAL-20000-0.txt"))
        input_file_list.append(("TwoPL-CXL-improved", ycsb_res_dir + "/ycsb-TwoPL-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-NoMoveOut-OnDemand-0-0-NoOP-None-GROUP_WAL-20000-0.txt"))
        return input_file_list

def parse_ycsb(ycsb_res_dir, motor_ycsb_csv, rw_ratio, zipf_theta):
        input_file_list = construct_input_list_ycsb(ycsb_res_dir, rw_ratio, zipf_theta)
        output_file_name = ycsb_res_dir + "/ycsb-" + rw_ratio + "-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_results(input_file_list, output_file_name, header_row)
        # add Motor numbers
        append_motor_numbers(output_file_name, motor_ycsb_csv)


if len(sys.argv) != 2:
        print("Usage: " + sys.argv[0] + " RESULT_ROOT_DIR")
        sys.exit(-1)

res_root_dir = sys.argv[1]
ycsb_res_dir = res_root_dir + "/ycsb"
script_path = os.path.abspath(__file__)
script_directory = os.path.dirname(script_path)
motor_ycsb_dir = script_directory + "/../../results/motor"

### Tigon and baselines ###
parse_ycsb(ycsb_res_dir, motor_ycsb_dir + "/ycsb-100-0.7.csv", "100", "0.7")
parse_ycsb(ycsb_res_dir, motor_ycsb_dir + "/ycsb-0-0.7.csv", "0", "0.7")
parse_ycsb(ycsb_res_dir, motor_ycsb_dir + "/ycsb-95-0.7.csv", "95", "0.7")
parse_ycsb(ycsb_res_dir, motor_ycsb_dir + "/ycsb-50-0.7.csv", "50", "0.7")
