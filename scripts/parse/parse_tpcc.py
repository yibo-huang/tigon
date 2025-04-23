#!/usr/bin/env python3

import sys
import csv
import fileinput
import pandas as pd
import os

from common import get_row, parse_results, append_motor_numbers

### baselines only ###
def construct_input_list_tpcc_baseline(res_dir):
        input_file_list = list()
        input_file_list.append(("Sundial-CXL-improved", res_dir + "/tpcc-Sundial-8-3-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0-0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL-20000-0" + ".txt"))
        input_file_list.append(("TwoPL-CXL-improved", res_dir + "/tpcc-TwoPL-8-3-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0-0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL-20000-0" + ".txt"))
        input_file_list.append(("Sundial-CXL", res_dir + "/tpcc-Sundial-8-2-1-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0-0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL-20000-0" + ".txt"))
        input_file_list.append(("TwoPL-CXL", res_dir + "/tpcc-TwoPL-8-2-1-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0-0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL-20000-0" + ".txt"))
        input_file_list.append(("Sundial-NET", res_dir + "/tpcc-Sundial-8-2-0-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0-0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL-20000-0" + ".txt"))
        input_file_list.append(("TwoPL-NET", res_dir + "/tpcc-TwoPL-8-2-0-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0-0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL-20000-0" + ".txt"))
        return input_file_list

def parse_tpcc_baseline(res_dir):
        input_file_list = construct_input_list_tpcc_baseline(res_dir)
        output_file_name = res_dir + "/baseline-tpcc.csv"
        header_row = ["Remote_Ratio", "0/0", "10/15", "20/30", "30/45", "40/60", "50/75", "60/90"]
        parse_results(input_file_list, output_file_name, header_row)


### Tigon and baselines ###
def construct_input_list_tpcc(tigon_res_dir, baseline_res_dir):
        input_file_list = list()
        input_file_list.append(("Tigon", tigon_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-Phantom", tigon_res_dir + "/tpcc-TwoPLPashaPhantom-8-3-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))

        input_file_list.append(("Sundial-CXL-improved", baseline_res_dir + "/tpcc-Sundial-8-3-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0-0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL-20000-0" + ".txt"))
        input_file_list.append(("TwoPL-CXL-improved", baseline_res_dir + "/tpcc-TwoPL-8-3-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0-0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL-20000-0" + ".txt"))
        return input_file_list

def parse_tpcc(tigon_res_dir, baseline_res_dir, motor_res_dir):
        input_file_list = construct_input_list_tpcc(tigon_res_dir, baseline_res_dir)
        output_file_name = tigon_res_dir + "/tpcc.csv"
        header_row = ["Remote_Ratio", "0/0", "10/15", "20/30", "30/45", "40/60", "50/75", "60/90"]
        parse_results(input_file_list, output_file_name, header_row)
        # add Motor numbers
        motor_csv_name = motor_res_dir + "/tpcc.csv"
        append_motor_numbers(output_file_name, motor_csv_name)


if len(sys.argv) != 4:
        print("Usage: " + sys.argv[0] + " tigon_res_dir baseline_res_dir motor_res_dir")
        sys.exit(-1)

tigon_res_dir = sys.argv[1]
baseline_res_dir = sys.argv[2]
motor_res_dir = sys.argv[3]

### baseline only ###
parse_ycsb_baseline(baseline_res_dir + "/micro", "100", "0.7")
parse_ycsb_baseline(baseline_res_dir + "/micro", "0", "0.7")
parse_ycsb_baseline(baseline_res_dir + "/macro", "95", "0.7")
parse_ycsb_baseline(baseline_res_dir + "/macro", "50", "0.7")
parse_tpcc_baseline(baseline_res_dir + "/macro")

### Tigon and baselines ###
parse_tpcc(tigon_res_dir + "/macro", baseline_res_dir + "/macro", motor_res_dir)
