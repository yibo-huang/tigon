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
                if len(tokens) > 7 and tokens[3] == "Coordinator.h:500]":
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


### End-to-End Performance ###
def construct_input_list_tpcc_remote_txn_overhead(res_dir):
        input_file_list = list()
        # Pasha
        input_file_list.append(("Tigon", res_dir + "/tpcc-TwoPLPasha-8-2-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "WriteThrough" + "-" + "NonPart" + ".txt"))

        # Baselines
        input_file_list.append(("Sundial-CXL", res_dir + "/tpcc-Sundial-8-2-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + ".txt"))
        input_file_list.append(("TwoPL-CXL", res_dir + "/tpcc-TwoPL-8-2-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + ".txt"))
        input_file_list.append(("Sundial-NET", res_dir + "/tpcc-Sundial-8-2-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + ".txt"))
        input_file_list.append(("TwoPL-NET", res_dir + "/tpcc-TwoPL-8-2-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + ".txt"))
        return input_file_list

def construct_input_list_ycsb_remote_txn_overhead(res_dir, rw_ratio, zipf_theta):
        input_file_list = list()
        # Pasha
        input_file_list.append(("Tigon", res_dir + "/ycsb-TwoPLPasha-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "WriteThrough" + "-" + "NonPart" + ".txt"))

        # Baselines
        input_file_list.append(("Sundial-CXL", res_dir + "/ycsb-Sundial-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + ".txt"))
        input_file_list.append(("TwoPL-CXL", res_dir + "/ycsb-TwoPL-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + ".txt"))
        input_file_list.append(("Sundial-NET", res_dir + "/ycsb-Sundial-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + ".txt"))
        input_file_list.append(("TwoPL-NET", res_dir + "/ycsb-TwoPL-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + ".txt"))
        return input_file_list

def construct_input_list_custom_ycsb(res_dir, rw_ratio, zipf_theta):
        input_file_list = list()
        input_file_list.append(("Tigon", res_dir + "/ycsb-TwoPLPasha-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "WriteThrough" + "-" + "NonPart" + ".txt"))
        return input_file_list

def parse_tpcc_remote_txn_overhead(res_dir):
        input_file_list = construct_input_list_tpcc_remote_txn_overhead(res_dir)
        output_file_name = res_dir + "/tpcc-remote-txn-overhead.csv"
        header_row = ["Remote_Ratio", "0/0", "10/15", "20/30", "30/45", "40/60", "50/75", "60/90"]
        parse_results(input_file_list, output_file_name, header_row)

def parse_ycsb_remote_txn_overhead(res_dir, rw_ratio, zipf_theta):
        input_file_list = construct_input_list_ycsb_remote_txn_overhead(res_dir, rw_ratio, zipf_theta)
        output_file_name = res_dir + "/ycsb-rmw-remote-txn-overhead-" + rw_ratio + "-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_results(input_file_list, output_file_name, header_row)

def parse_ycsb_custom_remote_txn_overhead(res_dir, rw_ratio, zipf_theta):
        input_file_list = construct_input_list_custom_ycsb(res_dir, rw_ratio, zipf_theta)
        output_file_name = res_dir + "/ycsb-custom-remote-txn-overhead-" + rw_ratio + "-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_results(input_file_list, output_file_name, header_row)


### SCC Microbenchmark ###
def construct_input_list_tpcc_scc_overhead(res_dir):
        input_file_list = list()
        input_file_list.append(("Tigon-WT", res_dir + "/tpcc-TwoPLPasha-8-2-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "WriteThrough" + "-" + "NonPart" + ".txt"))
        input_file_list.append(("Tigon-HCC", res_dir + "/tpcc-TwoPLPasha-8-2-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "NonPart" + ".txt"))
        input_file_list.append(("Tigon-NT", res_dir + "/tpcc-TwoPLPasha-8-2-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NonTemporal" + "-" + "NonPart" + ".txt"))
        return input_file_list

def construct_input_list_ycsb_scc_overhead(res_dir, rw_ratio, zipf_theta):
        input_file_list = list()
        input_file_list.append(("Tigon-WT", res_dir + "/ycsb-TwoPLPasha-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "WriteThrough" + "-" + "NonPart" + ".txt"))
        input_file_list.append(("Tigon-HCC", res_dir + "/ycsb-TwoPLPasha-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "NonPart" + ".txt"))
        input_file_list.append(("Tigon-NT", res_dir + "/ycsb-TwoPLPasha-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NonTemporal" + "-" + "NonPart" + ".txt"))
        return input_file_list

def parse_tpcc_scc_overhead(res_dir):
        input_file_list = construct_input_list_tpcc_scc_overhead(res_dir)
        output_file_name = res_dir + "/tpcc-scc-overhead.csv"
        header_row = ["Remote_Ratio", "0/0", "10/15", "20/30", "30/45", "40/60", "50/75", "60/90"]
        parse_results(input_file_list, output_file_name, header_row)

def parse_ycsb_scc_overhead(res_dir, rw_ratio, zipf_theta):
        input_file_list = construct_input_list_ycsb_scc_overhead(res_dir, rw_ratio, zipf_theta)
        output_file_name = res_dir + "/ycsb-rmw-scc-overhead-" + rw_ratio + "-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_results(input_file_list, output_file_name, header_row)


### Migration Policy Microbenchmark ###
def construct_input_list_tpcc_migration_policy(res_dir):
        input_file_list = list()
        input_file_list.append(("Tigon-LRU", res_dir + "/tpcc-TwoPLPasha-8-2-1-" + "LRU" + "-" + "OnDemand" + "-" + "10000000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))
        input_file_list.append(("Tigon-FIFO", res_dir + "/tpcc-TwoPLPasha-8-2-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))
        input_file_list.append(("Tigon-NoMoveOut", res_dir + "/tpcc-TwoPLPasha-8-2-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "WriteThrough" + "-" + "NonPart" + ".txt"))
        return input_file_list

def construct_input_list_ycsb_migration_policy(res_dir, rw_ratio, zipf_theta):
        input_file_list = list()
        input_file_list.append(("Tigon-LRU", res_dir + "/ycsb-TwoPLPasha-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "LRU" + "-" + "OnDemand" + "-" + "1000000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))
        input_file_list.append(("Tigon-FIFO", res_dir + "/ycsb-TwoPLPasha-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "1000000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))
        input_file_list.append(("Tigon-NoMoveOut", res_dir + "/ycsb-TwoPLPasha-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "WriteThrough" + "-" + "NonPart" + ".txt"))
        return input_file_list

def parse_tpcc_migration_policy(res_dir):
        input_file_list = construct_input_list_tpcc_migration_policy(res_dir)
        output_file_name = res_dir + "/tpcc-migration-policy.csv"
        header_row = ["Remote_Ratio", "0/0", "10/15", "20/30", "30/45", "40/60", "50/75", "60/90"]
        parse_results(input_file_list, output_file_name, header_row)

def parse_ycsb_migration_policy(res_dir, rw_ratio, zipf_theta):
        input_file_list = construct_input_list_ycsb_migration_policy(res_dir, rw_ratio, zipf_theta)
        output_file_name = res_dir + "/ycsb-rmw-migration-policy-" + rw_ratio + "-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_results(input_file_list, output_file_name, header_row)

### Dynamic Data Movement ###
def construct_input_list_ycsb_dynamic_data_movement(res_dir, rw_ratio, zipf_theta):
        input_file_list = list()
        input_file_list.append(("Tigon-LRU-1000", res_dir + "/ycsb-TwoPLPasha-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "LRU" + "-" + "OnDemand" + "-" + "1000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))
        input_file_list.append(("Tigon-LRU-10000", res_dir + "/ycsb-TwoPLPasha-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "LRU" + "-" + "OnDemand" + "-" + "10000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))
        input_file_list.append(("Tigon-LRU-100000", res_dir + "/ycsb-TwoPLPasha-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "LRU" + "-" + "OnDemand" + "-" + "100000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))
        input_file_list.append(("Tigon-LRU-1000000", res_dir + "/ycsb-TwoPLPasha-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "LRU" + "-" + "OnDemand" + "-" + "1000000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))
        input_file_list.append(("Tigon-LRU-10000000", res_dir + "/ycsb-TwoPLPasha-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "LRU" + "-" + "OnDemand" + "-" + "10000000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))
        return input_file_list

def parse_ycsb_dynamic_data_movement(res_dir, rw_ratio, zipf_theta):
        input_file_list = construct_input_list_ycsb_dynamic_data_movement(res_dir, rw_ratio, zipf_theta)
        output_file_name = res_dir + "/ycsb-rmw-dyanmic_data_movement-" + rw_ratio + "-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_results(input_file_list, output_file_name, header_row)



if len(sys.argv) != 2:
        print("Usage: ./parse_numbers.py res_dir")
        sys.exit(-1)

res_dir = sys.argv[1]

### End-to-End Performance
parse_tpcc_remote_txn_overhead(res_dir)

parse_ycsb_remote_txn_overhead(res_dir, "50", "0")
parse_ycsb_remote_txn_overhead(res_dir, "50", "0.99")
parse_ycsb_remote_txn_overhead(res_dir, "90", "0")
parse_ycsb_remote_txn_overhead(res_dir, "90", "0.99")

parse_ycsb_custom_remote_txn_overhead(res_dir, "50", "0")
parse_ycsb_custom_remote_txn_overhead(res_dir, "50", "0.99")
parse_ycsb_custom_remote_txn_overhead(res_dir, "90", "0")
parse_ycsb_custom_remote_txn_overhead(res_dir, "90", "0.99")


### SCC Macrobenchmark
parse_tpcc_scc_overhead(res_dir)

parse_ycsb_scc_overhead(res_dir, "50", "0")
parse_ycsb_scc_overhead(res_dir, "50", "0.99")
parse_ycsb_scc_overhead(res_dir, "90", "0")
parse_ycsb_scc_overhead(res_dir, "90", "0.99")


### Migration Policy
parse_tpcc_migration_policy(res_dir)

parse_ycsb_migration_policy(res_dir, "50", "0")
parse_ycsb_migration_policy(res_dir, "50", "0.99")
parse_ycsb_migration_policy(res_dir, "90", "0")
parse_ycsb_migration_policy(res_dir, "90", "0.99")


### Dyanmic Data Movement
parse_ycsb_dynamic_data_movement(res_dir, "90", "0")
