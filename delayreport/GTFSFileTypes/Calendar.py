"""
Python representation of the GTFS calendar.txt file
"""

from GTFSFile import GTFSFile

class Calendar(GTFSFile):
    def __init__(self, file_path):
        """
        Instantiate calendar representation
        """
        GTFSFile.__init__(self, file_path, index_name="service_id")
