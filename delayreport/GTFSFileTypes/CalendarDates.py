"""
Python representation of the GTFS calendar_dates.txt file
"""

from GTFSFile import GTFSFile

class CalendarDates(GTFSFile):
    def __init__(self, file_path):
        """
        Instantiate calendar_dates representation
        """
        GTFSFile.__init__(self, file_path, index_name="service_id")
