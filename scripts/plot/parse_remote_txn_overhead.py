#!/usr/bin/env python3

import sys
import csv
import fileinput
import statistics

def construct_input_list_tpcc(res_dir, migration_policy, when_to_move_out, max_migrated_row_size):
        input_file_list = list()
        # CXL Transport
        input_file_list.append(("SundialPasha-CXL", res_dir + "/tpcc-SundialPasha-8-2-1-" + migration_policy + "-" + when_to_move_out + "-" + max_migrated_row_size + ".txt"))
        input_file_list.append(("Sundial-CXL", res_dir + "/tpcc-Sundial-8-2-1-" + migration_policy + "-" + when_to_move_out + "-" + max_migrated_row_size + ".txt"))
        # input_file_list.append(("Lotus-CXL", res_dir + "/tpcc-Lotus-8-2-1-" + migration_policy + "-" + when_to_move_out + "-" + max_migrated_row_size + ".txt"))
        # Network Transport
        input_file_list.append(("SundialPasha-NET", res_dir + "/tpcc-SundialPasha-8-2-0-" + migration_policy + "-" + when_to_move_out + "-" + max_migrated_row_size + ".txt"))
        input_file_list.append(("Sundial-NET", res_dir + "/tpcc-Sundial-8-2-0-" + migration_policy + "-" + when_to_move_out + "-" + max_migrated_row_size + ".txt"))
        # input_file_list.append(("Lotus-NET", res_dir + "/tpcc-Lotus-8-2-0-" + migration_policy + "-" + when_to_move_out + "-" + max_migrated_row_size + ".txt"))
        return input_file_list

def construct_input_list_ycsb(res_dir, zipf_theta, migration_policy, when_to_move_out, max_migrated_row_size):
        input_file_list = list()
        # CXL Transport
        input_file_list.append(("SundialPasha-CXL", res_dir + "/ycsb-SundialPasha-8-2-50-" + zipf_theta + "-1-" + migration_policy + "-" + when_to_move_out + "-" + max_migrated_row_size + ".txt"))
        input_file_list.append(("Sundial-CXL", res_dir + "/ycsb-Sundial-8-2-50-" + zipf_theta + "-1-" + migration_policy + "-" + when_to_move_out + "-" + max_migrated_row_size + ".txt"))
        # input_file_list.append(("Lotus", res_dir + "/ycsb-Lotus-8-2-50-" + zipf_theta + "-1-" + migration_policy + "-" + when_to_move_out + "-" + max_migrated_row_size + ".txt"))
        # Network Transport
        input_file_list.append(("SundialPasha-NET", res_dir + "/ycsb-SundialPasha-8-2-50-" + zipf_theta + "-0-" + migration_policy + "-" + when_to_move_out + "-" + max_migrated_row_size + ".txt"))
        input_file_list.append(("Sundial-NET", res_dir + "/ycsb-Sundial-8-2-50-" + zipf_theta + "-0-" + migration_policy + "-" + when_to_move_out + "-" + max_migrated_row_size + ".txt"))
        # input_file_list.append(("Lotus", res_dir + "/ycsb-Lotus-8-2-50-" + zipf_theta + "-0-" + migration_policy + "-" + when_to_move_out + "-" + max_migrated_row_size + ".txt"))
        return input_file_list

def get_row(input):
        tputs = list()
        tputs.append(input[0])

        # tput, CXL_usage_index, CXL_usage_data, CXL_usage_transport
        for line in fileinput.FileInput(input[1]):
                tokens = line.strip().split()
                if len(tokens) > 7 and tokens[3] == "Coordinator.h:492]":
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

def parse_tpcc(res_dir, migration_policy, when_to_move_out, max_migrated_row_size):
        input_file_list = construct_input_list_tpcc(res_dir, migration_policy, when_to_move_out, max_migrated_row_size)
        output_file_name = res_dir + "/tpcc.csv"
        header_row = ["Remote_Ratio", "0/0", "10/15", "20/30", "30/45", "40/60", "50/75", "60/90"]
        parse_results(input_file_list, output_file_name, header_row)

def parse_ycsb(res_dir, zipf_theta, migration_policy, when_to_move_out, max_migrated_row_size):
        input_file_list = construct_input_list_ycsb(res_dir, zipf_theta, migration_policy, when_to_move_out, max_migrated_row_size)
        output_file_name = res_dir + "/ycsb-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_results(input_file_list, output_file_name, header_row)


if len(sys.argv) != 2:
        print("Usage: ./parse_remote_txn_overhead.py res_dir")
        sys.exit(-1)

res_dir = sys.argv[1]

parse_tpcc(res_dir, "NoMoveOut", "OnDemand", "0")
parse_ycsb(res_dir, "0", "NoMoveOut", "OnDemand", "0")
parse_ycsb(res_dir, "0.99", "NoMoveOut", "OnDemand", "0")
