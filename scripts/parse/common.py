#!/usr/bin/env python3

import sys
import csv
import fileinput
import pandas as pd
import os

def get_row(input):
        tputs = list()
        tputs.append(input[0])

        # tput, CXL_usage_index, CXL_usage_data, CXL_usage_transport
        for line in fileinput.FileInput(input[1]):
                tokens = line.strip().split()
                if len(tokens) > 7 and tokens[3] == "Coordinator.h:610]":
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

def append_motor_numbers(output_file_name, motor_csv_name):
        orig_df = pd.read_csv(output_file_name)
        motor_df = pd.read_csv(motor_csv_name, usecols=['Motor'])
        orig_df = pd.concat([orig_df, motor_df], axis=1)
        orig_df.to_csv(output_file_name, index=False)
