#!/usr/bin/env python3

import sys
import csv
import fileinput
import statistics

def construct_input_list_with_scc(input_file_list, entry_name, orig_file_name):
        input_file_list.append((entry_name + "-WriteThrough", orig_file_name + "-WriteThrough" + ".txt"))
        input_file_list.append((entry_name + "-NoOP", orig_file_name + "-NoOP" + ".txt"))
        input_file_list.append((entry_name + "-NonTemporal", orig_file_name + "-NonTemporal" + ".txt"))


def construct_input_list_tpcc(res_dir, remote_neworder, remote_payment):
        input_file_list = list()

        construct_input_list_with_scc(input_file_list, "NoMoveOut-OnDemand-0", res_dir + "/tpcc-SundialPasha-" + remote_neworder + "-" + remote_payment + "-NoMoveOut-OnDemand-0")


        construct_input_list_with_scc(input_file_list, "Eagerly-OnDemand-80K", res_dir + "/tpcc-SundialPasha-" + remote_neworder + "-" + remote_payment + "-Eagerly-OnDemand-10000")

        construct_input_list_with_scc(input_file_list, "Eagerly-OnDemand-800K", res_dir + "/tpcc-SundialPasha-" + remote_neworder + "-" + remote_payment + "-Eagerly-OnDemand-100000")
        construct_input_list_with_scc(input_file_list, "Eagerly-OnDemand-8MB", res_dir + "/tpcc-SundialPasha-" + remote_neworder + "-" + remote_payment + "-Eagerly-OnDemand-1000000")
        construct_input_list_with_scc(input_file_list, "Eagerly-OnDemand-80MB", res_dir + "/tpcc-SundialPasha-" + remote_neworder + "-" + remote_payment + "-Eagerly-OnDemand-10000000")
        construct_input_list_with_scc(input_file_list, "Eagerly-OnDemand-800MB", res_dir + "/tpcc-SundialPasha-" + remote_neworder + "-" + remote_payment + "-Eagerly-OnDemand-100000000")

        construct_input_list_with_scc(input_file_list, "Eagerly-Reactive-1", res_dir + "/tpcc-SundialPasha-" + remote_neworder + "-" + remote_payment + "-Eagerly-Reactive-1")

        construct_input_list_with_scc(input_file_list, "OnDemandFIFO-OnDemand-80K", res_dir + "/tpcc-SundialPasha-" + remote_neworder + "-" + remote_payment + "-OnDemandFIFO-OnDemand-10000")
        construct_input_list_with_scc(input_file_list, "OnDemandFIFO-OnDemand-800K", res_dir + "/tpcc-SundialPasha-" + remote_neworder + "-" + remote_payment + "-OnDemandFIFO-OnDemand-100000")
        construct_input_list_with_scc(input_file_list, "OnDemandFIFO-OnDemand-8MB", res_dir + "/tpcc-SundialPasha-" + remote_neworder + "-" + remote_payment + "-OnDemandFIFO-OnDemand-1000000")
        construct_input_list_with_scc(input_file_list, "OnDemandFIFO-OnDemand-80MB", res_dir + "/tpcc-SundialPasha-" + remote_neworder + "-" + remote_payment + "-OnDemandFIFO-OnDemand-10000000")
        construct_input_list_with_scc(input_file_list, "OnDemandFIFO-OnDemand-800MB", res_dir + "/tpcc-SundialPasha-" + remote_neworder + "-" + remote_payment + "-OnDemandFIFO-OnDemand-100000000")

        return input_file_list

def construct_input_list_ycsb(res_dir, zipf_theta, cross_ratio):
        input_file_list = list()

        construct_input_list_with_scc(input_file_list, "NoMoveOut-OnDemand-0", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-" + cross_ratio + "-NoMoveOut-OnDemand-0")

        construct_input_list_with_scc(input_file_list, "Eagerly-OnDemand-80K", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-" + cross_ratio + "-Eagerly-OnDemand-10000")
        construct_input_list_with_scc(input_file_list, "Eagerly-OnDemand-800K", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-" + cross_ratio + "-Eagerly-OnDemand-100000")
        construct_input_list_with_scc(input_file_list, "Eagerly-OnDemand-8MB", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-" + cross_ratio + "-Eagerly-OnDemand-1000000")
        construct_input_list_with_scc(input_file_list, "Eagerly-OnDemand-80MB", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-" + cross_ratio + "-Eagerly-OnDemand-10000000")
        construct_input_list_with_scc(input_file_list, "Eagerly-OnDemand-800MB", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-" + cross_ratio + "-Eagerly-OnDemand-100000000")

        construct_input_list_with_scc(input_file_list, "Eagerly-Reactive-1", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-" + cross_ratio + "-Eagerly-Reactive-1")

        construct_input_list_with_scc(input_file_list, "OnDemandFIFO-OnDemand-80K", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-" + cross_ratio + "-OnDemandFIFO-OnDemand-10000")
        construct_input_list_with_scc(input_file_list, "OnDemandFIFO-OnDemand-800K", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-" + cross_ratio + "-OnDemandFIFO-OnDemand-100000")
        construct_input_list_with_scc(input_file_list, "OnDemandFIFO-OnDemand-8MB", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-" + cross_ratio + "-OnDemandFIFO-OnDemand-1000000")
        construct_input_list_with_scc(input_file_list, "OnDemandFIFO-OnDemand-80MB", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-" + cross_ratio + "-OnDemandFIFO-OnDemand-10000000")
        construct_input_list_with_scc(input_file_list, "OnDemandFIFO-OnDemand-800MB", res_dir + "/ycsb-SundialPasha-" + zipf_theta + "-" + cross_ratio + "-OnDemandFIFO-OnDemand-100000000")

        return input_file_list

def get_row(input):
        row = list()
        row.append(input[0])

        # tput, CXL_usage_index, CXL_usage_data, CXL_usage_transport
        for line in fileinput.FileInput(input[1]):
                tokens = line.strip().split()
                if len(tokens) > 7 and tokens[3] == "Coordinator.h:496]":
                        row.append(tokens[7])
                        row.append(tokens[9])
                        row.append(tokens[11])
                        row.append(tokens[13])

        # msg_exchange number, size
        for line in fileinput.FileInput(input[1]):
                tokens = line.strip().split()
                if len(tokens) > 7 and tokens[6] == "type:" and tokens[7] == "4":
                        row.append(tokens[9])
                        row.append(tokens[12])

        # abort_rate, local_CXL_access_ratio, remote_access_with_req_ratio, data_move_in_num, data_move_out_num
        for line in fileinput.FileInput(input[1]):
                tokens = line.strip().split()
                if len(tokens) > 3 and tokens[3] == "Coordinator.h:275]":
                        row.append(tokens[11].replace(",", ""))
                        row.append(tokens[30].replace(",", "").replace("(", "").replace(")", ""))
                        row.append(tokens[35].replace(",", "").replace("(", "").replace(")", ""))
                        row.append(tokens[37].replace(",", ""))
                        row.append(tokens[39].replace(",", ""))

        # data_move_in_size, data_move_out_size
        for line in fileinput.FileInput(input[1]):
                tokens = line.strip().split()
                if len(tokens) > 7 and tokens[3] == "CXLMemory.h:132]":
                        row.append(tokens[16])
                        row.append(tokens[18])

        # num_clflush, num_clwb, num_cache_hit, num_cache_miss, cache_hit_rate
        for line in fileinput.FileInput(input[1]):
                tokens = line.strip().split()
                if len(tokens) > 7 and tokens[3] == "SCCManager.h:37]":
                        row.append(tokens[8])
                        row.append(tokens[10])
                        row.append(tokens[12])
                        row.append(tokens[14])
                        row.append(tokens[18])

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

def parse_tpcc(res_dir, remote_neworder, remote_payment):
        input_file_list = construct_input_list_tpcc(res_dir, remote_neworder, remote_payment)
        output_file_name = res_dir + "/tpcc.csv"
        header_row = ["Policy", "tput", "CXL_usage_index", "CXL_usage_data", "CXL_usage_transport",
                      "msg_exchange_num", "msg_exchange_size", "abort_rate", "local_cxl_access_ratio",
                      "remote_access_with_req_ratio", "data_move_in_num", "data_move_out_num",
                      "data_move_in_size", "data_move_out_size",
                      "num_clflush", "num_clwb", "num_cache_hit", "num_cache_miss", "cache_hit_rate"]
        parse_results(input_file_list, output_file_name, header_row)

def parse_ycsb(res_dir, zipf_theta, cross_ratio):
        input_file_list = construct_input_list_ycsb(res_dir, zipf_theta, cross_ratio)
        output_file_name = res_dir + "/ycsb-" + zipf_theta + ".csv"
        header_row = ["Policy", "tput", "CXL_usage_index", "CXL_usage_data", "CXL_usage_transport",
                      "msg_exchange_num", "msg_exchange_size", "abort_rate", "local_cxl_access_ratio",
                      "remote_access_with_req_ratio", "data_move_in_num", "data_move_out_num",
                      "data_move_in_size", "data_move_out_size",
                      "num_clflush", "num_clwb", "num_cache_hit", "num_cache_miss", "cache_hit_rate"]
        parse_results(input_file_list, output_file_name, header_row)


if len(sys.argv) != 2:
        print("Usage: ./parse_migration_policy_stats.py res_dir")
        sys.exit(-1)

res_dir = sys.argv[1]

parse_tpcc(res_dir, "10", "15")

parse_ycsb(res_dir, "0", "10")
parse_ycsb(res_dir, "0.99", "10")
