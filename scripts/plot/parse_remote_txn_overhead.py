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


### remote txn overhead ###
def construct_input_list_tpcc_remote_txn_overhead(res_dir):
        input_file_list = list()
        # Pasha
        input_file_list.append(("SundialPasha-CXL", res_dir + "/tpcc-SundialPasha-8-2-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))
        input_file_list.append(("TwoPLPasha-CXL", res_dir + "/tpcc-TwoPLPasha-8-2-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))

        # Baselines
        input_file_list.append(("Sundial-CXL", res_dir + "/tpcc-Sundial-8-2-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + ".txt"))
        input_file_list.append(("TwoPL-CXL", res_dir + "/tpcc-TwoPL-8-2-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + ".txt"))
        input_file_list.append(("Sundial-NET", res_dir + "/tpcc-Sundial-8-2-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + ".txt"))
        input_file_list.append(("TwoPL-NET", res_dir + "/tpcc-TwoPL-8-2-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + ".txt"))
        return input_file_list

def construct_input_list_ycsb_remote_txn_overhead(res_dir, rw_ratio, zipf_theta):
        input_file_list = list()
        # Pasha
        input_file_list.append(("SundialPasha-CXL", res_dir + "/ycsb-SundialPasha-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))
        input_file_list.append(("TwoPLPasha-CXL", res_dir + "/ycsb-TwoPLPasha-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))

        # Baselines
        input_file_list.append(("Sundial-CXL", res_dir + "/ycsb-Sundial-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + ".txt"))
        input_file_list.append(("TwoPL-CXL", res_dir + "/ycsb-TwoPL-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + ".txt"))
        input_file_list.append(("Sundial-NET", res_dir + "/ycsb-Sundial-8-2-" + rw_ratio + "-" + zipf_theta + "-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + ".txt"))
        input_file_list.append(("TwoPL-NET", res_dir + "/ycsb-TwoPL-8-2-" + rw_ratio + "-" + zipf_theta + "-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + ".txt"))
        return input_file_list

def parse_tpcc_remote_txn_overhead(res_dir):
        input_file_list = construct_input_list_tpcc_remote_txn_overhead(res_dir)
        output_file_name = res_dir + "/tpcc-remote-txn-overhead.csv"
        header_row = ["Remote_Ratio", "0/0", "10/15", "20/30", "30/45", "40/60", "50/75", "60/90"]
        parse_results(input_file_list, output_file_name, header_row)

def parse_ycsb_remote_txn_overhead(res_dir, rw_ratio, zipf_theta):
        input_file_list = construct_input_list_ycsb_remote_txn_overhead(res_dir, rw_ratio, zipf_theta)
        output_file_name = res_dir + "/ycsb-remote-txn-overhead-" + rw_ratio + "-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_results(input_file_list, output_file_name, header_row)


### SCC Macrobenchmark ###
def construct_input_list_tpcc_scc_overhead(res_dir):
        input_file_list = list()
        input_file_list.append(("SundialPasha-CXL-WT", res_dir + "/tpcc-SundialPasha-8-2-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))
        input_file_list.append(("TwoPLPasha-CXL-WT", res_dir + "/tpcc-TwoPLPasha-8-2-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))
        input_file_list.append(("SundialPasha-CXL-HCC", res_dir + "/tpcc-SundialPasha-8-2-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000" + "-" + "NoOP" + "-" + "None" + ".txt"))
        input_file_list.append(("TwoPLPasha-CXL-HCC", res_dir + "/tpcc-TwoPLPasha-8-2-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000" + "-" + "NoOP" + "-" + "None" + ".txt"))
        input_file_list.append(("SundialPasha-CXL-NT", res_dir + "/tpcc-SundialPasha-8-2-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000" + "-" + "NonTemporal" + "-" + "None" + ".txt"))
        input_file_list.append(("TwoPLPasha-CXL-NT", res_dir + "/tpcc-TwoPLPasha-8-2-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000" + "-" + "NonTemporal" + "-" + "None" + ".txt"))
        return input_file_list

def construct_input_list_ycsb_scc_overhead(res_dir, rw_ratio, zipf_theta):
        input_file_list = list()
        input_file_list.append(("SundialPasha-CXL-WT", res_dir + "/ycsb-SundialPasha-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))
        input_file_list.append(("TwoPLPasha-CXL-WT", res_dir + "/ycsb-TwoPLPasha-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))
        input_file_list.append(("SundialPasha-CXL-HCC", res_dir + "/ycsb-SundialPasha-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000" + "-" + "NoOP" + "-" + "None" + ".txt"))
        input_file_list.append(("TwoPLPasha-CXL-HCC", res_dir + "/ycsb-TwoPLPasha-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000" + "-" + "NoOP" + "-" + "None" + ".txt"))
        input_file_list.append(("SundialPasha-CXL-NT", res_dir + "/ycsb-SundialPasha-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000" + "-" + "NonTemporal" + "-" + "None" + ".txt"))
        input_file_list.append(("TwoPLPasha-CXL-NT", res_dir + "/ycsb-TwoPLPasha-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000" + "-" + "NonTemporal" + "-" + "None" + ".txt"))
        return input_file_list

def parse_tpcc_scc_overhead(res_dir):
        input_file_list = construct_input_list_tpcc_scc_overhead(res_dir)
        output_file_name = res_dir + "/tpcc-scc-overhead.csv"
        header_row = ["Remote_Ratio", "0/0", "10/15", "20/30", "30/45", "40/60", "50/75", "60/90"]
        parse_results(input_file_list, output_file_name, header_row)

def parse_ycsb_scc_overhead(res_dir, rw_ratio, zipf_theta):
        input_file_list = construct_input_list_ycsb_scc_overhead(res_dir, rw_ratio, zipf_theta)
        output_file_name = res_dir + "/ycsb-scc-overhead-" + rw_ratio + "-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_results(input_file_list, output_file_name, header_row)


### Migration Policy Macrobenchmark ###
def construct_input_list_tpcc_migration_policy(res_dir):
        input_file_list = list()
        input_file_list.append(("SundialPasha-CXL-FIFO", res_dir + "/tpcc-SundialPasha-8-2-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))
        input_file_list.append(("TwoPLPasha-CXL-FIFO", res_dir + "/tpcc-TwoPLPasha-8-2-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))
        input_file_list.append(("SundialPasha-CXL-NoMoveOut", res_dir + "/tpcc-SundialPasha-8-2-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "WriteThrough" + "-" + "NonPart" + ".txt"))
        input_file_list.append(("TwoPLPasha-CXL-NoMoveOut", res_dir + "/tpcc-TwoPLPasha-8-2-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "WriteThrough" + "-" + "NonPart" + ".txt"))
        return input_file_list

def construct_input_list_ycsb_migration_policy(res_dir, rw_ratio, zipf_theta):
        input_file_list = list()
        input_file_list.append(("SundialPasha-CXL-FIFO", res_dir + "/ycsb-SundialPasha-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))
        input_file_list.append(("TwoPLPasha-CXL-FIFO", res_dir + "/ycsb-TwoPLPasha-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "OnDemandFIFO" + "-" + "OnDemand" + "-" + "10000" + "-" + "WriteThrough" + "-" + "None" + ".txt"))
        input_file_list.append(("SundialPasha-CXL-NoMoveOut", res_dir + "/ycsb-SundialPasha-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "WriteThrough" + "-" + "NonPart" + ".txt"))
        input_file_list.append(("TwoPLPasha-CXL-NoMoveOut", res_dir + "/ycsb-TwoPLPasha-8-2-" + rw_ratio + "-" + zipf_theta + "-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "WriteThrough" + "-" + "NonPart" + ".txt"))
        return input_file_list

def parse_tpcc_migration_policy(res_dir):
        input_file_list = construct_input_list_tpcc_migration_policy(res_dir)
        output_file_name = res_dir + "/tpcc-migration-policy.csv"
        header_row = ["Remote_Ratio", "0/0", "10/15", "20/30", "30/45", "40/60", "50/75", "60/90"]
        parse_results(input_file_list, output_file_name, header_row)

def parse_ycsb_migration_policy(res_dir, rw_ratio, zipf_theta):
        input_file_list = construct_input_list_ycsb_migration_policy(res_dir, rw_ratio, zipf_theta)
        output_file_name = res_dir + "/ycsb-migration-policy-" + rw_ratio + "-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_results(input_file_list, output_file_name, header_row)


if len(sys.argv) != 2:
        print("Usage: ./parse_numbers.py res_dir")
        sys.exit(-1)

res_dir = sys.argv[1]

### Remote TXN Overhead
parse_tpcc_remote_txn_overhead(res_dir)

parse_ycsb_remote_txn_overhead(res_dir, "50", "0")
parse_ycsb_remote_txn_overhead(res_dir, "50", "0.99")
parse_ycsb_remote_txn_overhead(res_dir, "90", "0")
parse_ycsb_remote_txn_overhead(res_dir, "90", "0.99")


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
