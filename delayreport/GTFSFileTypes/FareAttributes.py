"""
Python representation of the GTFS fare_attributes.txt file
"""

from GTFSFile import GTFSFile

class FareAttributes(GTFSFile):
    def __init__(self, file_path):
        """
        Instantiate fare_attributes representation
        """
        GTFSFile.__init__(self, file_path, index_name="fare_id")
