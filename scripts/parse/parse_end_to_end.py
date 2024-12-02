#!/usr/bin/env python3

import sys
import csv
import fileinput

def get_row(input):
        tputs = list()
        tputs.append(input[0])

        # tput, CXL_usage_index, CXL_usage_data, CXL_usage_transport
        for line in fileinput.FileInput(input[1]):
                tokens = line.strip().split()
                if len(tokens) > 7 and tokens[3] == "Coordinator.h:583]":
                        tputs.append(float(tokens[7]))

        return tputs

def parse_results(input_list, output_file_name, header_row):
        row = list()
        rows = list()

        # write header row first
        rows.append(header_row)

        # read all the files and construct the row
        for input in input_list:
                row = get_row(input)
                rows.append(row)

        # convert rows into columns
        rows = zip(*rows)

        with open(output_file_name, "w") as output_file:
                output_writer = csv.writer(output_file)
                output_writer.writerows(rows)

def construct_input_list_ycsb(pasha_res_dir, baseline_res_dir, rw_ratio, zipf_theta):
        input_file_list = list()
        input_file_list.append(("Tigon", pasha_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "WriteThrough" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))

        input_file_list.append(("Sundial-CXL-improved", baseline_res_dir + "/ycsb-Sundial-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        input_file_list.append(("TwoPL-CXL-improved", baseline_res_dir + "/ycsb-TwoPL-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        return input_file_list

def parse_ycsb(pasha_res_dir, baseline_res_dir, rw_ratio, zipf_theta):
        input_file_list = construct_input_list_ycsb(pasha_res_dir, baseline_res_dir, rw_ratio, zipf_theta)
        output_file_name = pasha_res_dir + "/e2e-ycsb-" + rw_ratio + "-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_results(input_file_list, output_file_name, header_row)

def construct_input_list_tpcc(pasha_res_dir, baseline_res_dir):
        input_file_list = list()
        input_file_list.append(("Tigon", pasha_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "WriteThrough" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        input_file_list.append(("Tigon-Phantom", pasha_res_dir + "/tpcc-TwoPLPashaPhantom-8-3-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "WriteThrough" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))

        input_file_list.append(("Sundial-CXL-improved", baseline_res_dir + "/tpcc-Sundial-8-3-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        input_file_list.append(("TwoPL-CXL-improved", baseline_res_dir + "/tpcc-TwoPL-8-3-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        return input_file_list

def parse_tpcc(pasha_res_dir, baseline_res_dir):
        input_file_list = construct_input_list_tpcc(pasha_res_dir, baseline_res_dir)
        output_file_name = pasha_res_dir + "/e2e-tpcc.csv"
        header_row = ["Remote_Ratio", "0/0", "10/15", "20/30", "30/45", "40/60", "50/75", "60/90"]
        parse_results(input_file_list, output_file_name, header_row)


if len(sys.argv) != 3:
        print("Usage: " + sys.argv[0] + " pasha_res_dir baseline_res_dir")
        sys.exit(-1)

pasha_res_dir = sys.argv[1]
baseline_res_dir = sys.argv[2]

parse_ycsb(pasha_res_dir + "/micro", baseline_res_dir + "/micro", "100", "0.7")
parse_ycsb(pasha_res_dir + "/micro", baseline_res_dir + "/micro", "0", "0.7")

parse_ycsb(pasha_res_dir + "/macro", baseline_res_dir + "/macro", "95", "0.7")
parse_ycsb(pasha_res_dir + "/macro", baseline_res_dir + "/macro", "50", "0.7")

parse_tpcc(pasha_res_dir + "/macro", baseline_res_dir + "/macro")
