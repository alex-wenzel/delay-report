"""
Python representation of the GTFS agency.txt file
"""

from GTFSFile import GTFSFile

class Agency(GTFSFile):
    def __init__(self, file_path):
        """
        Instantiate Agency representation
        """
        GTFSFile.__init__(self, file_path, index_name="agency_id")
