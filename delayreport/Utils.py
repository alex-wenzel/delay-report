import datetime
import time

def get_unix_time(gtfs_time):
    """
    Generates a unix timestamp for a time from a GTFS entry

        gtfs_time (str): A string in "hh:mm:ss"

        returns (float): The unix timestamp represented by this time
    """
    base_day = get_time_today_midnight()
    hour, minute, second = list(map(int, gtfs_time.split(':')))
    return base_day + (3600*hour) + (60*minute) + second

def get_time_today_midnight():
    """
    Returns the the timestamp for today's date, 12:00am

        returns (float): The unix timestamp for today at midnight
    """
    now = datetime.datetime.now()
    return(time.mktime(datetime.date(year = now.year, month = now.month, day = now.day).timetuple()))
