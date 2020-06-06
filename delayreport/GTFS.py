"""
Implements a python representation of a GTFS static transit feed
https://developers.google.com/transit/gtfs/reference/
"""

from GTFSFileTypes.Agency import Agency
from GTFSFileTypes.Calendar import Calendar
from GTFSFileTypes.CalendarDates import CalendarDates
from GTFSFileTypes.FareAttributes import FareAttributes
from GTFSFileTypes.FareRules import FareRules
from GTFSFileTypes.Frequencies import Frequencies
from GTFSFileTypes.Routes import Routes
from GTFSFileTypes.Shapes import Shapes
from GTFSFileTypes.StopTimes import StopTimes
from GTFSFileTypes.Stops import Stops
from GTFSFileTypes.Trips import Trips

class GTFS:
    def __init__(self, gtfs_dir_path):
        """
        Starts parsing of gtfs files at gtfs_dir_path
        """
        if gtfs_dir_path[-1] != '/':
            gtfs_dir_path += '/'

        self.agency = Agency(gtfs_dir_path+"agency.txt")
        self.calendar = Calendar(gtfs_dir_path+"calendar.txt")
        self.calendar_dates = CalendarDates(gtfs_dir_path+"calendar_dates.txt")
        self.fare_attributes = FareAttributes(gtfs_dir_path+"fare_attributes.txt")
        self.fare_rules = FareRules(gtfs_dir_path+"fare_rules.txt")
        self.frequencies = Frequencies(gtfs_dir_path+"frequencies.txt")
        self.routes = Routes(gtfs_dir_path+"routes.txt")
        self.shapes = Shapes(gtfs_dir_path+"shapes.txt")
        self.stop_times = StopTimes(gtfs_dir_path+"stop_times.txt")
        self.stops = Stops(gtfs_dir_path+"stops.txt")
        self.trips = Trips(gtfs_dir_path+"trips.txt")

if __name__ == "__main__":
    GTFS("../data/sample-feed/")
