"""
Functions for querying an OBA feed and parsing to pandas
"""

from google.transit import gtfs_realtime_pb2
import pandas as pd
import requests

def query_trip_update(url, api_key):
    """
    Returns the Python representation of the GTFS feed for trip updates

        url (str): API url
        api_key (str): api key

        returns (gtfs_realtime_pb2.FeedMessage): Python repr of pb data
    """
    feed = gtfs_realtime_pb2.FeedMessage()
    response = requests.get(url+"?key="+api_key)
    feed.ParseFromString(response.content)
    return feed

def parse_trip_updates(feed):
    """
    This function converts the protocol buffer data from trip updates into
    a useful pandas DataFrame

        feed (gtfs_realtime_pb2.FeedMessage): Python repr of pb data

        returns (pd.DataFrame): Pandas representation of pb data
    """
    tu_d = {
        "trip_id": [], "rte_id": [], "ns_id": [],
        "ns_depart": [], "veh_id": [], "delay": [],
        "ts": []
    }
    for e in feed.entity:
        tu_d['trip_id'].append(e.trip_update.trip.trip_id)
        tu_d['rte_id'].append(e.trip_update.trip.route_id)
        tu_d['ns_id'].append(e.trip_update.stop_time_update[0].stop_id)
        tu_d['ns_depart'].append(e.trip_update.stop_time_update[0].departure.time)
        tu_d['veh_id'].append(e.trip_update.vehicle.id)
        tu_d['delay'].append(e.trip_update.delay)#/60.0)
        tu_d['ts'].append(e.trip_update.timestamp)

    return pd.DataFrame(tu_d).set_index('trip_id')
