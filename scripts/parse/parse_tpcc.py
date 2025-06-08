#!/usr/bin/env python3

import sys
import csv
import fileinput
import pandas as pd
import os

from common import get_row, parse_results, append_motor_numbers

### baselines only ###
def construct_input_list_tpcc_baseline(tpcc_res_dir):
        input_file_list = list()
        input_file_list.append(("Sundial-CXL-improved", tpcc_res_dir + "/tpcc-Sundial-8-3-1-0-NoMoveOut-OnDemand-0-0-NoOP-None-GROUP_WAL-10000-0.txt"))
        input_file_list.append(("TwoPL-CXL-improved", tpcc_res_dir + "/tpcc-TwoPL-8-3-1-0-NoMoveOut-OnDemand-0-0-NoOP-None-GROUP_WAL-10000-0.txt"))
        input_file_list.append(("Sundial-CXL", tpcc_res_dir + "/tpcc-Sundial-8-2-1-1-NoMoveOut-OnDemand-0-0-NoOP-None-GROUP_WAL-10000-0.txt"))
        input_file_list.append(("TwoPL-CXL", tpcc_res_dir + "/tpcc-TwoPL-8-2-1-1-NoMoveOut-OnDemand-0-0-NoOP-None-GROUP_WAL-10000-0.txt"))
        input_file_list.append(("Sundial-NET", tpcc_res_dir + "/tpcc-Sundial-8-2-0-1-NoMoveOut-OnDemand-0-0-NoOP-None-GROUP_WAL-10000-0.txt"))
        input_file_list.append(("TwoPL-NET", tpcc_res_dir + "/tpcc-TwoPL-8-2-0-1-NoMoveOut-OnDemand-0-0-NoOP-None-GROUP_WAL-10000-0.txt"))
        return input_file_list

def parse_tpcc_baseline(tpcc_res_dir):
        input_file_list = construct_input_list_tpcc_baseline(tpcc_res_dir)
        output_file_name = tpcc_res_dir + "/baseline-tpcc.csv"
        header_row = ["Remote_Ratio", "0/0", "10/15", "20/30", "30/45", "40/60", "50/75", "60/90"]
        parse_results(input_file_list, output_file_name, header_row)


### Tigon and baselines ###
def construct_input_list_tpcc(tpcc_res_dir):
        input_file_list = list()
        input_file_list.append(("Tigon", tpcc_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-Clock-OnDemand-209715200-1-WriteThrough-NonPart-GROUP_WAL-10000-0.txt"))
        # input_file_list.append(("Tigon-Phantom", tpcc_res_dir + "/tpcc-TwoPLPashaPhantom-8-3-1-0-Clock-OnDemand-209715200-1-WriteThrough-NonPart-GROUP_WAL-10000-0.txt"))

        input_file_list.append(("Sundial-CXL-improved", tpcc_res_dir + "/tpcc-Sundial-8-3-1-0-NoMoveOut-OnDemand-0-0-NoOP-None-GROUP_WAL-10000-0.txt"))
        input_file_list.append(("TwoPL-CXL-improved", tpcc_res_dir + "/tpcc-TwoPL-8-3-1-0-NoMoveOut-OnDemand-0-0-NoOP-None-GROUP_WAL-10000-0.txt"))
        return input_file_list

def parse_tpcc(tpcc_res_dir, motor_tpcc_csv):
        input_file_list = construct_input_list_tpcc(tpcc_res_dir)
        output_file_name = tpcc_res_dir + "/tpcc.csv"
        header_row = ["Remote_Ratio", "0/0", "10/15", "20/30", "30/45", "40/60", "50/75", "60/90"]
        parse_results(input_file_list, output_file_name, header_row)
        # add Motor numbers
        append_motor_numbers(output_file_name, motor_tpcc_csv)


if len(sys.argv) != 2:
        print("Usage: " + sys.argv[0] + " RESULT_ROOT_DIR")
        sys.exit(-1)

res_root_dir = sys.argv[1]
tpcc_res_dir = res_root_dir + "/tpcc"
script_path = os.path.abspath(__file__)
script_directory = os.path.dirname(script_path)
motor_tpcc_csv = script_directory + "/../../results/motor/tpcc.csv"

### baseline only ###
parse_tpcc_baseline(tpcc_res_dir)

### Tigon and baselines ###
parse_tpcc(tpcc_res_dir, motor_tpcc_csv)
