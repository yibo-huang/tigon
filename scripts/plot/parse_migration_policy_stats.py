#!/usr/bin/env python3

import sys
import csv
import fileinput
import statistics

def construct_input_list_tpcc(res_dir):
        input_file_list = list()

        input_file_list.append(("Eagerly-OnDemand-100", res_dir + "/tpcc-SundialPasha-Eagerly-OnDemand-100.txt"))
        input_file_list.append(("Eagerly-OnDemand-1000", res_dir + "/tpcc-SundialPasha-Eagerly-OnDemand-1000.txt"))
        input_file_list.append(("Eagerly-OnDemand-10000", res_dir + "/tpcc-SundialPasha-Eagerly-OnDemand-10000.txt"))
        input_file_list.append(("Eagerly-OnDemand-50000", res_dir + "/tpcc-SundialPasha-Eagerly-OnDemand-50000.txt"))
        input_file_list.append(("Eagerly-OnDemand-100000", res_dir + "/tpcc-SundialPasha-Eagerly-OnDemand-100000.txt"))

        input_file_list.append(("Eagerly-Reactive-1", res_dir + "/tpcc-SundialPasha-Eagerly-Reactive-1.txt"))

        input_file_list.append(("OnDemandFIFO-OnDemand-100", res_dir + "/tpcc-SundialPasha-OnDemandFIFO-OnDemand-100.txt"))
        input_file_list.append(("OnDemandFIFO-OnDemand-1000", res_dir + "/tpcc-SundialPasha-OnDemandFIFO-OnDemand-1000.txt"))
        input_file_list.append(("OnDemandFIFO-OnDemand-10000", res_dir + "/tpcc-SundialPasha-OnDemandFIFO-OnDemand-10000.txt"))
        input_file_list.append(("OnDemandFIFO-OnDemand-50000", res_dir + "/tpcc-SundialPasha-OnDemandFIFO-OnDemand-50000.txt"))
        input_file_list.append(("OnDemandFIFO-OnDemand-100000", res_dir + "/tpcc-SundialPasha-OnDemandFIFO-OnDemand-100000.txt"))

        return input_file_list

def construct_input_list_ycsb(res_dir, zipf_theta):
        input_file_list = list()

        input_file_list.append(("Eagerly-OnDemand-100", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-Eagerly-OnDemand-100.txt"))
        input_file_list.append(("Eagerly-OnDemand-1000", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-Eagerly-OnDemand-1000.txt"))
        input_file_list.append(("Eagerly-OnDemand-10000", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-Eagerly-OnDemand-10000.txt"))
        input_file_list.append(("Eagerly-OnDemand-50000", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-Eagerly-OnDemand-50000.txt"))
        input_file_list.append(("Eagerly-OnDemand-100000", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-Eagerly-OnDemand-100000.txt"))

        input_file_list.append(("Eagerly-Reactive-1", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-Eagerly-Reactive-1.txt"))

        input_file_list.append(("OnDemandFIFO-OnDemand-100", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-OnDemandFIFO-OnDemand-100.txt"))
        input_file_list.append(("OnDemandFIFO-OnDemand-1000", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-OnDemandFIFO-OnDemand-1000.txt"))
        input_file_list.append(("OnDemandFIFO-OnDemand-10000", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-OnDemandFIFO-OnDemand-10000.txt"))
        input_file_list.append(("OnDemandFIFO-OnDemand-50000", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-OnDemandFIFO-OnDemand-50000.txt"))
        input_file_list.append(("OnDemandFIFO-OnDemand-100000", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-OnDemandFIFO-OnDemand-100000.txt"))

        return input_file_list

def get_row(input):
        row = list()
        row.append(input[0])
        # tput
        for line in fileinput.FileInput(input[1]):
                tokens = line.strip().split()
                if len(tokens) == 7 and tokens[4] == "total" and tokens[5] == "commit:":
                        row.append(float(tokens[6]))

        # msg_exchange number, size
        for line in fileinput.FileInput(input[1]):
                tokens = line.strip().split()
                if len(tokens) > 7 and tokens[6] == "type:" and tokens[7] == "4":
                        row.append(tokens[9])
                        row.append(tokens[12])

        # abort_rate, local_CXL_access_ratio, remote_access_with_req_ratio, data_move_in, data_move_out
        for line in fileinput.FileInput(input[1]):
                tokens = line.strip().split()
                if len(tokens) > 3 and tokens[3] == "Coordinator.h:275]":
                        row.append(tokens[11].replace(",", ""))
                        row.append(tokens[30].replace(",", "").replace("(", "").replace(")", ""))
                        row.append(tokens[35].replace(",", "").replace("(", "").replace(")", ""))
                        row.append(tokens[37].replace(",", ""))
                        row.append(tokens[39])

        return row

def parse_results(input_list, output_file_name, header_row):
        row = list()
        rows = list()

        # write header row first
        rows.append(header_row)

        # read all the files and construct the row
        for input in input_list:
                row = get_row(input)
                rows.append(row)

        # # convert rows into columns
        # rows = zip(*rows)

        with open(output_file_name, "w") as output_file:
                output_writer = csv.writer(output_file)
                output_writer.writerows(rows)

def parse_tpcc(res_dir):
        input_file_list = construct_input_list_tpcc(res_dir)
        output_file_name = res_dir + "/tpcc.csv"
        header_row = ["Policy", "tput", "msg_exchange_num", "msg_exchange_size", "abort_rate", "local_cxl_access_ratio", "remote_access_with_req_ratio", "data_move_in", "data_move_out"]
        parse_results(input_file_list, output_file_name, header_row)

def parse_ycsb(res_dir, zipf_theta):
        input_file_list = construct_input_list_ycsb(res_dir, zipf_theta)
        output_file_name = res_dir + "/ycsb-" + zipf_theta + ".csv"
        header_row = ["Policy", "tput", "msg_exchange_num", "msg_exchange_size", "abort_rate", "local_cxl_access_ratio", "remote_access_with_req_ratio", "data_move_in", "data_move_out"]
        parse_results(input_file_list, output_file_name, header_row)


if len(sys.argv) != 2:
        print("Usage: ./parse_migration_policy_stats.py res_dir")
        sys.exit(-1)

res_dir = sys.argv[1]

parse_tpcc(res_dir)
parse_ycsb(res_dir, "0")
parse_ycsb(res_dir, "0.99")
