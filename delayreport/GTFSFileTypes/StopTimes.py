"""
Python representation of the GTFS stop_times.txt file
"""

from GTFSFile import GTFSFile
import Utils

class StopTimes(GTFSFile):
    def __init__(self, file_path):
        """
        Instantiate stop_times representation
        """
        GTFSFile.__init__(self, file_path)
        #st_df["departure_time"] = st_df["departure_time"].apply(get_unix_time)
        self.data["departure_time"] = self.data["departure_time"].apply(Utils.get_unix_time)
