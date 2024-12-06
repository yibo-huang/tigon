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
                if len(tokens) > 7 and tokens[3] == "Coordinator.h:586]":
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

def get_latency_p50(input):
        lat_p50 = list()
        lat_p50.append(input[0])

        # tput, CXL_usage_index, CXL_usage_data, CXL_usage_transport
        for line in fileinput.FileInput(input[1]):
                tokens = line.strip().split()
                if len(tokens) > 7 and tokens[3] == "WALLogger.h:539]":
                        lat_p50.append(float(tokens[7]))

        return lat_p50

def get_latency_p99(input):
        lat_p50 = list()
        lat_p50.append(input[0])

        # tput, CXL_usage_index, CXL_usage_data, CXL_usage_transport
        for line in fileinput.FileInput(input[1]):
                tokens = line.strip().split()
                if len(tokens) > 7 and tokens[3] == "WALLogger.h:539]":
                        lat_p50.append(float(tokens[16]))

        return lat_p50

def parse_latency(input_list, output_file_name, header_row):
        lat_p50_row = list()
        lat_p50_rows = list()
        lat_p99_row = list()
        lat_p99_rows = list()

        # write header row first
        lat_p50_rows.append(header_row)
        lat_p99_rows.append(header_row)

        # read all the files and construct the row
        for input in input_list:
                lat_p50_row = get_latency_p50(input)
                lat_p50_rows.append(lat_p50_row)
                lat_p99_row = get_latency_p99(input)
                lat_p99_rows.append(lat_p99_row)

        # convert rows into columns
        # lat_p50_rows = zip(*lat_p50_rows)
        # lat_p99_rows = zip(*lat_p99_rows)

        with open(output_file_name, "w") as output_file:
                output_writer = csv.writer(output_file)
                output_writer.writerows(lat_p50_rows)
                output_writer.writerows(lat_p99_rows)




### baseline only ###
def construct_input_list_ycsb_baseline(res_dir, rw_ratio, zipf_theta):
        input_file_list = list()
        input_file_list.append(("Sundial-CXL-improved", res_dir + "/ycsb-Sundial-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        input_file_list.append(("TwoPL-CXL-improved", res_dir + "/ycsb-TwoPL-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        input_file_list.append(("Sundial-CXL", res_dir + "/ycsb-Sundial-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-1-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        input_file_list.append(("TwoPL-CXL", res_dir + "/ycsb-TwoPL-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-1-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        input_file_list.append(("Sundial-NET", res_dir + "/ycsb-Sundial-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-0-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        input_file_list.append(("TwoPL-NET", res_dir + "/ycsb-TwoPL-rmw-8-2-" + rw_ratio + "-" + zipf_theta + "-0-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        return input_file_list

def parse_ycsb_baseline(res_dir, rw_ratio, zipf_theta):
        input_file_list = construct_input_list_ycsb_baseline(res_dir, rw_ratio, zipf_theta)
        output_file_name = res_dir + "/baseline-ycsb-" + rw_ratio + "-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_results(input_file_list, output_file_name, header_row)

def construct_input_list_tpcc_baseline(res_dir):
        input_file_list = list()
        input_file_list.append(("Sundial-CXL-improved", res_dir + "/tpcc-Sundial-8-3-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        input_file_list.append(("TwoPL-CXL-improved", res_dir + "/tpcc-TwoPL-8-3-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        input_file_list.append(("Sundial-CXL", res_dir + "/tpcc-Sundial-8-2-1-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        input_file_list.append(("TwoPL-CXL", res_dir + "/tpcc-TwoPL-8-2-1-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        input_file_list.append(("Sundial-NET", res_dir + "/tpcc-Sundial-8-2-0-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        input_file_list.append(("TwoPL-NET", res_dir + "/tpcc-TwoPL-8-2-0-1-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        return input_file_list

def parse_tpcc_baseline(res_dir):
        input_file_list = construct_input_list_tpcc_baseline(res_dir)
        output_file_name = res_dir + "/baseline-tpcc.csv"
        header_row = ["Remote_Ratio", "0/0", "10/15", "20/30", "30/45", "40/60", "50/75", "60/90"]
        parse_results(input_file_list, output_file_name, header_row)




### pasha and baselines ###
def construct_input_list_ycsb(pasha_res_dir, baseline_res_dir, rw_ratio, zipf_theta):
        input_file_list = list()
        input_file_list.append(("Tigon", pasha_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))

        input_file_list.append(("Sundial-CXL-improved", baseline_res_dir + "/ycsb-Sundial-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        input_file_list.append(("TwoPL-CXL-improved", baseline_res_dir + "/ycsb-TwoPL-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        return input_file_list

def parse_ycsb(pasha_res_dir, baseline_res_dir, rw_ratio, zipf_theta):
        input_file_list = construct_input_list_ycsb(pasha_res_dir, baseline_res_dir, rw_ratio, zipf_theta)
        output_file_name = pasha_res_dir + "/ycsb-" + rw_ratio + "-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_results(input_file_list, output_file_name, header_row)

def construct_input_list_ycsb_custom(pasha_res_dir, zipf_theta):
        input_file_list = list()
        input_file_list.append(("Tigon", pasha_res_dir + "/ycsb-TwoPLPasha-mixed-8-3-100" + "-" + zipf_theta + "-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        return input_file_list

def parse_ycsb_custom(pasha_res_dir, zipf_theta):
        input_file_list = construct_input_list_ycsb_custom(pasha_res_dir, zipf_theta)
        output_file_name = pasha_res_dir + "/ycsb-custom-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_results(input_file_list, output_file_name, header_row)

def construct_input_list_ycsb_with_read_cxl(pasha_res_dir, baseline_res_dir, rw_ratio, zipf_theta):
        input_file_list = list()
        input_file_list.append(("Tigon", pasha_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-ReadCXL", pasha_res_dir + "/ycsb-TwoPLPashaReadCXL-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))

        input_file_list.append(("Sundial-CXL-improved", baseline_res_dir + "/ycsb-Sundial-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        input_file_list.append(("TwoPL-CXL-improved", baseline_res_dir + "/ycsb-TwoPL-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        return input_file_list

def parse_ycsb_with_read_cxl(pasha_res_dir, baseline_res_dir, rw_ratio, zipf_theta):
        input_file_list = construct_input_list_ycsb_with_read_cxl(pasha_res_dir, baseline_res_dir, rw_ratio, zipf_theta)
        output_file_name = pasha_res_dir + "/ycsb-with-read-cxl-" + rw_ratio + "-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_results(input_file_list, output_file_name, header_row)

def construct_input_list_tpcc(pasha_res_dir, baseline_res_dir):
        input_file_list = list()
        input_file_list.append(("Tigon", pasha_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-Phantom", pasha_res_dir + "/tpcc-TwoPLPashaPhantom-8-3-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))

        input_file_list.append(("Sundial-CXL-improved", baseline_res_dir + "/tpcc-Sundial-8-3-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        input_file_list.append(("TwoPL-CXL-improved", baseline_res_dir + "/tpcc-TwoPL-8-3-1-0-" + "NoMoveOut" + "-" + "OnDemand" + "-" + "0" + "-" + "NoOP" + "-" + "None" + "-" + "GROUP_WAL" + ".txt"))
        return input_file_list

def parse_tpcc(pasha_res_dir, baseline_res_dir):
        input_file_list = construct_input_list_tpcc(pasha_res_dir, baseline_res_dir)
        output_file_name = pasha_res_dir + "/tpcc.csv"
        header_row = ["Remote_Ratio", "0/0", "10/15", "20/30", "30/45", "40/60", "50/75", "60/90"]
        parse_results(input_file_list, output_file_name, header_row)




### shortcut optimization ###
def construct_input_list_ycsb_shortcut(pasha_res_dir, pasha_no_shortcut_res_dir, rw_ratio, zipf_theta):
        input_file_list = list()
        input_file_list.append(("Tigon", pasha_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-AlwaysSearchCXL", pasha_no_shortcut_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-20000" + "-" + "1" + ".txt"))
        return input_file_list

def parse_ycsb_shortcut(pasha_res_dir, pasha_no_shortcut_res_dir, rw_ratio, zipf_theta):
        input_file_list = construct_input_list_ycsb_shortcut(pasha_res_dir, pasha_no_shortcut_res_dir, rw_ratio, zipf_theta)
        output_file_name = pasha_no_shortcut_res_dir + "/ycsb-shortcut-" + rw_ratio + "-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_results(input_file_list, output_file_name, header_row)

def construct_input_list_tpcc_shortcut(pasha_res_dir, pasha_no_shortcut_res_dir):
        input_file_list = list()
        input_file_list.append(("Tigon", pasha_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-AlwaysSearchCXL", pasha_no_shortcut_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-20000" + "-" + "1" + ".txt"))
        return input_file_list

def parse_tpcc_shortcut(pasha_res_dir, pasha_no_shortcut_res_dir):
        input_file_list = construct_input_list_tpcc_shortcut(pasha_res_dir, pasha_no_shortcut_res_dir)
        output_file_name = pasha_no_shortcut_res_dir + "/tpcc-shortcut.csv"
        header_row = ["Remote_Ratio", "0/0", "10/15", "20/30", "30/45", "40/60", "50/75", "60/90"]
        parse_results(input_file_list, output_file_name, header_row)




### data movement ###
def construct_input_list_ycsb_data_movement(pasha_res_dir, pasha_data_movement_res_dir, rw_ratio, zipf_theta):
        input_file_list = list()
        input_file_list.append(("Tigon-200MB", pasha_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-150MB", pasha_data_movement_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "157286400" + "-" + "1-WriteThrough" + "-" + "None" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-100MB", pasha_data_movement_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "104857600" + "-" + "1-WriteThrough" + "-" + "None" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-50MB", pasha_data_movement_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "52428800" + "-" + "1-WriteThrough" + "-" + "None" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-10MB", pasha_data_movement_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "10485760" + "-" + "1-WriteThrough" + "-" + "None" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        return input_file_list

def parse_ycsb_data_movement(pasha_res_dir, pasha_data_movement_res_dir, rw_ratio, zipf_theta):
        input_file_list = construct_input_list_ycsb_data_movement(pasha_res_dir, pasha_data_movement_res_dir, rw_ratio, zipf_theta)
        output_file_name = pasha_data_movement_res_dir + "/ycsb-data-movement-" + rw_ratio + "-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_results(input_file_list, output_file_name, header_row)

def construct_input_list_tpcc_data_movement(pasha_res_dir, pasha_data_movement_res_dir):
        input_file_list = list()
        input_file_list.append(("Tigon-200MB", pasha_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-150MB", pasha_data_movement_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "157286400" + "-" + "1-WriteThrough" + "-" + "None" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-100MB", pasha_data_movement_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "104857600" + "-" + "1-WriteThrough" + "-" + "None" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-50MB", pasha_data_movement_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "52428800" + "-" + "1-WriteThrough" + "-" + "None" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-10MB", pasha_data_movement_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "10485760" + "-" + "1-WriteThrough" + "-" + "None" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        return input_file_list

def parse_tpcc_data_movement(pasha_res_dir, pasha_data_movement_res_dir):
        input_file_list = construct_input_list_tpcc_data_movement(pasha_res_dir, pasha_data_movement_res_dir)
        output_file_name = pasha_data_movement_res_dir + "/tpcc-data-movement.csv"
        header_row = ["Remote_Ratio", "0/0", "10/15", "20/30", "30/45", "40/60", "50/75", "60/90"]
        parse_results(input_file_list, output_file_name, header_row)




### logging ###
def construct_input_list_ycsb_logging(pasha_logging_res_dir, rw_ratio, zipf_theta):
        input_file_list = list()
        input_file_list.append(("Tigon-no-logging", pasha_logging_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "BLACKHOLE-0" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-1ms", pasha_logging_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-1000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-10ms", pasha_logging_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-10000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-20ms", pasha_logging_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-30ms", pasha_logging_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-30000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-40ms", pasha_logging_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-40000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-50ms", pasha_logging_res_dir + "/ycsb-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-50000" + "-" + "0" + ".txt"))
        return input_file_list

def parse_ycsb_logging(pasha_logging_res_dir, rw_ratio, zipf_theta):
        input_file_list = construct_input_list_ycsb_logging(pasha_logging_res_dir, rw_ratio, zipf_theta)
        output_file_name = pasha_logging_res_dir + "/ycsb-logging-" + rw_ratio + "-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_results(input_file_list, output_file_name, header_row)

        output_file_name = pasha_logging_res_dir + "/ycsb-logging-latency-" + rw_ratio + "-" + zipf_theta + ".csv"
        header_row = ["Remote_Ratio", "0", "10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]
        parse_latency(input_file_list, output_file_name, header_row)

def construct_input_list_tpcc_logging(pasha_logging_res_dir):
        input_file_list = list()
        input_file_list.append(("Tigon-no-logging", pasha_logging_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "BLACKHOLE-0" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-1ms", pasha_logging_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-1000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-10ms", pasha_logging_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-10000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-20ms", pasha_logging_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-30ms", pasha_logging_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-30000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-40ms", pasha_logging_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-40000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-50ms", pasha_logging_res_dir + "/tpcc-TwoPLPasha-8-3-1-0-" + "Clock" + "-" + "OnDemand" + "-" + "209715200" + "-" + "1-WriteThrough" + "-" + "NonPart" + "-" + "GROUP_WAL-50000" + "-" + "0" + ".txt"))
        return input_file_list

def parse_tpcc_logging(pasha_logging_res_dir):
        input_file_list = construct_input_list_tpcc_logging(pasha_logging_res_dir)
        output_file_name = pasha_logging_res_dir + "/tpcc-logging.csv"
        header_row = ["Remote_Ratio", "0/0", "10/15", "20/30", "30/45", "40/60", "50/75", "60/90"]
        parse_results(input_file_list, output_file_name, header_row)

        output_file_name = pasha_logging_res_dir + "/tpcc-logging-latency.csv"
        header_row = ["Remote_Ratio", "0/0", "10/15", "20/30", "30/45", "40/60", "50/75", "60/90"]
        parse_latency(input_file_list, output_file_name, header_row)





### SCC ###
def construct_input_list_ycsb_scc(pasha_res_dir, pasha_scc_res_dir, rw_ratio, zipf_theta):
        input_file_list = list()
        input_file_list.append(("Tigon", pasha_scc_res_dir + "/ycsb-scc-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-1-" + "Clock" + "-" + "OnDemand" + "-" + "1-WriteThrough" + "-" + "None" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-AlwaysMemcpy", pasha_scc_res_dir + "/ycsb-scc-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-0-" + "Clock" + "-" + "OnDemand" + "-" + "1-WriteThrough" + "-" + "None" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-NoSharedReader", pasha_scc_res_dir + "/ycsb-scc-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-1-" + "Clock" + "-" + "OnDemand" + "-" + "1-WriteThroughNoSharedRead" + "-" + "None" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-NonTemporal", pasha_scc_res_dir + "/ycsb-scc-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-1-" + "Clock" + "-" + "OnDemand" + "-" + "1-NonTemporal" + "-" + "None" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-NoSCC", pasha_scc_res_dir + "/ycsb-scc-TwoPLPasha-rmw-8-3-" + rw_ratio + "-" + zipf_theta + "-1-0-1-" + "Clock" + "-" + "OnDemand" + "-" + "0-NoOP" + "-" + "None" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        return input_file_list

def parse_ycsb_scc(pasha_res_dir, pasha_scc_res_dir, rw_ratio, zipf_theta):
        input_file_list = construct_input_list_ycsb_scc(pasha_res_dir, pasha_scc_res_dir, rw_ratio, zipf_theta)
        output_file_name = pasha_scc_res_dir + "/ycsb-scc-" + rw_ratio + "-" + zipf_theta + ".csv"
        header_row = ["HWcc", "200MB", "150MB", "100MB", "50MB", "10MB"]
        parse_results(input_file_list, output_file_name, header_row)

def construct_input_list_tpcc_scc(pasha_res_dir, pasha_scc_res_dir):
        input_file_list = list()
        input_file_list.append(("Tigon", pasha_scc_res_dir + "/tpcc-scc-TwoPLPasha-8-3-1-0-1-" + "Clock" + "-" + "OnDemand" + "-" + "1-WriteThrough" + "-" + "None" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-AlwaysMemcpy", pasha_scc_res_dir + "/tpcc-scc-TwoPLPasha-8-3-1-0-0-" + "Clock" + "-" + "OnDemand" + "-" + "1-WriteThrough" + "-" + "None" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-NoSharedReader", pasha_scc_res_dir + "/tpcc-scc-TwoPLPasha-8-3-1-0-1-" + "Clock" + "-" + "OnDemand" + "-" + "1-WriteThroughNoSharedRead" + "-" + "None" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-NonTemporal", pasha_scc_res_dir + "/tpcc-scc-TwoPLPasha-8-3-1-0-1-" + "Clock" + "-" + "OnDemand" + "-" + "1-NonTemporal" + "-" + "None" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        input_file_list.append(("Tigon-NoSCC", pasha_scc_res_dir + "/tpcc-scc-TwoPLPasha-8-3-1-0-1-" + "Clock" + "-" + "OnDemand" + "-" + "0-NoOP" + "-" + "None" + "-" + "GROUP_WAL-20000" + "-" + "0" + ".txt"))
        return input_file_list

def parse_tpcc_scc(pasha_res_dir, pasha_scc_res_dir):
        input_file_list = construct_input_list_tpcc_scc(pasha_res_dir, pasha_scc_res_dir)
        output_file_name = pasha_scc_res_dir + "/tpcc-scc.csv"
        header_row = ["HWcc", "200MB", "150MB", "100MB", "50MB", "10MB"]
        parse_results(input_file_list, output_file_name, header_row)



if len(sys.argv) != 3:
        print("Usage: " + sys.argv[0] + " pasha_res_dir baseline_res_dir")
        sys.exit(-1)

pasha_res_dir = sys.argv[1]
baseline_res_dir = sys.argv[2]

### baseline only ###
parse_ycsb_baseline(baseline_res_dir + "/micro", "100", "0.7")
parse_ycsb_baseline(baseline_res_dir + "/micro", "0", "0.7")
parse_ycsb_baseline(baseline_res_dir + "/macro", "95", "0.7")
parse_ycsb_baseline(baseline_res_dir + "/macro", "50", "0.7")
parse_tpcc_baseline(baseline_res_dir + "/macro")

### pasha and baselines ###
parse_ycsb_with_read_cxl(pasha_res_dir + "/micro", baseline_res_dir + "/micro", "100", "0.7")
parse_ycsb_custom(pasha_res_dir + "/micro", "0.7")
parse_ycsb(pasha_res_dir + "/micro", baseline_res_dir + "/micro", "100", "0.7")
parse_ycsb(pasha_res_dir + "/micro", baseline_res_dir + "/micro", "0", "0.7")
parse_ycsb(pasha_res_dir + "/macro", baseline_res_dir + "/macro", "95", "0.7")
parse_ycsb(pasha_res_dir + "/macro", baseline_res_dir + "/macro", "50", "0.7")
parse_tpcc(pasha_res_dir + "/macro", baseline_res_dir + "/macro")

### shortcut optimization ###
parse_ycsb_shortcut(pasha_res_dir + "/macro", pasha_res_dir + "/shortcut", "95", "0.7")
parse_tpcc_shortcut(pasha_res_dir + "/macro", pasha_res_dir + "/shortcut")

### data movement
parse_tpcc_data_movement(pasha_res_dir + "/macro", pasha_res_dir + "/data-movement")
parse_ycsb_data_movement(pasha_res_dir + "/macro", pasha_res_dir + "/data-movement", "95", "0.7")
parse_ycsb_data_movement(pasha_res_dir + "/macro", pasha_res_dir + "/data-movement", "50", "0.7")

### logging ###
parse_ycsb_logging(pasha_res_dir + "/logging", "50", "0.7")
parse_tpcc_logging(pasha_res_dir + "/logging")

### scc ###
parse_ycsb_scc(pasha_res_dir + "/macro", pasha_res_dir + "/scc", "95", "0.7")
parse_tpcc_scc(pasha_res_dir + "/macro", pasha_res_dir + "/scc")
