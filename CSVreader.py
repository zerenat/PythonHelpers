"""
CSVreader.py
Author: Martin Hein (@heimarti)
Date created: 30/05/2022
Date last modified: 02/06/2022
Version: 1.01
Python Version: 3.9
"""

import csv


class CSVreader:
    @staticmethod
    def read_csv(path):
        with open(path, newline='') as csvfile:
            file_reader = csv.reader(csvfile, delimiter='`')
            file_rows = [x[0].split(',') for x in file_reader]
            file_data = {}
            for i in range(0, len(file_rows[0])):
                key = file_rows[0][i]
                if len(key) <= 0:
                    continue
                values = []
                for o in range(1, len(file_rows)):
                    values.append(file_rows[o][i])
                file_data[key] = values
        return file_data
