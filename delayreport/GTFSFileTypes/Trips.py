"""
Python representation of the GTFS trips.txt file
"""

from GTFSFile import GTFSFile

class Trips(GTFSFile):
    def __init__(self, file_path):
        """
        Instantiate trips representation
        """
        GTFSFile.__init__(self, file_path, index_name="trip_id")
