"""
Python representation of the GTFS stops.txt file
"""

from GTFSFile import GTFSFile

class Stops(GTFSFile):
    def __init__(self, file_path):
        """
        Instantiate stops representation
        """
        GTFSFile.__init__(self, file_path, index_name="stop_id")
