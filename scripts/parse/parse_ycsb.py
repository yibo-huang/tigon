#!/usr/bin/env python3

import sys
import csv
import fileinput
import pandas as pd
import os

from common import get_row, parse_results, append_motor_numbers

### Tigon and baselines ###
def construct_input_list_ycsb(tigon_res_dir, baseline_res_dir, rw_ratio, zipf_theta):
        input_file_list = list()
        input_file_list.append(("Tigon", tigon_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))

        input_file_list.append(("Sundial-CXL-improved", baseline_res_dir + "/ycsb-Sundial-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0-0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL-20000-0" + ".txt"))
        input_file_list.append(("TwoPL-CXL-improved", baseline_res_dir + "/ycsb-TwoPL-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0-0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL-20000-0" + ".txt"))
        return input_file_list

def parse_ycsb(tigon_res_dir, baseline_res_dir, motor_res_dir, rw_ratio, zipf_theta):
        input_file_list = construct_input_list_ycsb(tigon_res_dir, baseline_res_dir, rw_ratio, zipf_theta)
        output_file_name = tigon_res_dir + "/ycsb-" + rw_ratio + "-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_results(input_file_list, output_file_name, header_row)
        # add Motor numbers
        motor_csv_name = motor_res_dir + "/ycsb-" + rw_ratio + "-" + zipf_theta + ".csv"
        append_motor_numbers(output_file_name, motor_csv_name)

if len(sys.argv) != 4:
        print("Usage: " + sys.argv[0] + " tigon_res_dir baseline_res_dir motor_res_dir")
        sys.exit(-1)

tigon_res_dir = sys.argv[1]
baseline_res_dir = sys.argv[2]
motor_res_dir = sys.argv[3]

### Tigon and baselines ###
parse_ycsb(tigon_res_dir + "/micro", baseline_res_dir + "/micro", motor_res_dir, "100", "0.7")
parse_ycsb(tigon_res_dir + "/micro", baseline_res_dir + "/micro", motor_res_dir, "0", "0.7")
parse_ycsb(tigon_res_dir + "/macro", baseline_res_dir + "/macro", motor_res_dir, "95", "0.7")
parse_ycsb(tigon_res_dir + "/macro", baseline_res_dir + "/macro", motor_res_dir, "50", "0.7")
