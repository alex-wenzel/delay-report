"""
Parent class for GTFS files, loads the file as a pandas DataFrame
"""

from pandas import read_csv

class GTFSFile:
    def __init__(self, file_path, index_name=None):
        """
        Read data as a DataFrame
        """
        self.data = read_csv(file_path, dtype=str)
        if index_name is not None:
            self.data = self.data.set_index(index_name)

    def __iter__(self):
        for i, row in self.data.iterrows():
            yield row

    def enumerate(self):
        for i, row in self.data.iterrows():
            yield i, row
