"""
Python representation of the GTFS routes.txt file
"""

from GTFSFile import GTFSFile

class Routes(GTFSFile):
    def __init__(self, file_path):
        """
        Instantiate routes representation
        """
        GTFSFile.__init__(self, file_path, index_name="route_id")
