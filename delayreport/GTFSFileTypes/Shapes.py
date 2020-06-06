"""
Python representation of the GTFS shapes.txt file
"""

from GTFSFile import GTFSFile

class Shapes(GTFSFile):
    def __init__(self, file_path):
        """
        Instantiate shapes representation
        """
        GTFSFile.__init__(self, file_path, index_name="shape_id")
