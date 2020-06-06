"""
Python representation of the GTFS frequencies.txt file
"""

from GTFSFile import GTFSFile

class Frequencies(GTFSFile):
    def __init__(self, file_path):
        """
        Instantiate frequencies representation
        """
        GTFSFile.__init__(self, file_path)
