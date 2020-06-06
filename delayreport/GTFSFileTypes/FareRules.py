"""
Python representation of the GTFS fare_rules.txt file
"""

from GTFSFile import GTFSFile

class FareRules(GTFSFile):
    def __init__(self, file_path):
        """
        Instantiate fare_rules representation
        """
        GTFSFile.__init__(self, file_path, index_name="fare_id")
