"""
This script runs the reporter
"""

import copy
import datetime
from google.transit import gtfs_realtime_pb2
import json
import pandas as pd
import sys
import time

import GTFS
import Log
import OBA
import Utils

def get_current_trips(gtfs):
    """
    Gets trips that should be active at this moment

        gtfs (GTFS.GTFS): Representation of the GTFS object

        returns (pd.Series): Active trip IDs
    """
    NOW = time.time()
    base_day = Utils.get_time_today_midnight()

    ## get trips dataframe
    t_df = gtfs.trips.data

    ## get stop_times dataframe
    st_df = gtfs.stop_times.data

    ## convert timestamps to linux
    #st_df["departure_time"] = st_df["departure_time"].apply(Utils.get_unix_time)

    ## get only trips today
    st_df = st_df[(st_df["departure_time"] > base_day) & (st_df["departure_time"] < base_day+86400)]
    today_tid = list(set(st_df["trip_id"]))
    t_df = t_df[t_df.index.isin(today_tid)]
    #t_df = t_df[t_df["service_id"] == "70522-1111100-0"]
    svc_ids = [
        "70522-1111100-0", "70523-1111100-0", "70524-1111100-0",
        "70526-1111100-0", "70525-1111100-0", "70507-1111100-0",
        "70504-1111100-0", "70488-1111100-0", "70849-1111100-0"
    ]
    #t_df = t_df[t_df["service_id"].isin(svc_ids)]
    t_df = t_df[t_df["service_id"].str.contains("1111100")]
    t_df = t_df[~t_df["route_id"].isin(["510", "520", "530"])]

    t_df = pd.merge(t_df, st_df, on = 'trip_id')

    starts = t_df[(t_df["stop_sequence"] == "1")]
    ends = t_df[(t_df["stop_is_last"] == "1")]

    maybe_active_start_ids = starts[starts["departure_time"] < NOW]["trip_id"]
    maybe_active_end_ids = ends[ends["departure_time"] > NOW]["trip_id"]

    active_ids = list(set(maybe_active_start_ids).intersection(maybe_active_end_ids))

    return active_ids

def check_delays_missing(gtfs, feed, curr_trips, now, delay_thresh = 10):
    """
    This function checks the GTFS for expected trips and
    """
    delayed = []
    missing = []

    for trip_id in curr_trips:
        try:
            feed_record = feed.loc[trip_id,:]
            #if type(feed_record) == pd.DataFrame:
            #    print(feed_record)
        except KeyError:
            s_df = gtfs.stop_times.data
            trip_stops = copy.deepcopy(s_df[s_df["trip_id"]==trip_id])
            #trip_stops["departure_unix_time"] = trip_stops["departure_time"].apply(Utils.get_unix_time)
            trip_stops["time_from_now"] = trip_stops["departure_time"].apply(lambda x: abs(x - now))
            closest_stop = trip_stops[trip_stops["time_from_now"]==trip_stops["time_from_now"].min()]
            if closest_stop.iloc[0,:]["stop_sequence"] != "1" and closest_stop.iloc[0,:]["stop_is_last"] == "0":
                missing.append([gtfs.trips.data.loc[trip_id,:], closest_stop.iloc[0,:]])
            continue

        ## Get next stop from feed
        real_ns_id = feed_record["ns_id"]

        ## Get departure time for next stop from feed
        try:
            real_depart = float(feed_record["ns_depart"])
        except TypeError:
            #print(feed_record["ns_depart"])
            #print("103 type error")
            continue

        ## Get departure time for next stop from gtfs
        stops = gtfs.stop_times.data
        stops = stops[stops["trip_id"] == trip_id]
        #sched_stop_depart = float(stops[stops["stop_is_last"]=="1"]["departure_time"])
        try:
            sched_stop_depart = float(stops[stops["stop_id"]==feed_record["ns_id"]]["departure_time"])
        except TypeError:
            #print(stops[stops["stop_id"]==feed_record["ns_id"]]["departure_time"])
            #print("103 type error")
            continue
        #print(sched_stop_depart)

        ## Subtract to find difference
        delay = real_depart - sched_stop_depart

        #print(feed_record["rte_id"], real_depart, sched_stop_depart, delay)

        ## Check threshold and add
        if delay >= delay_thresh*60.0:
            feed_record = copy.deepcopy(feed_record)
            feed_record["delay"] = delay/60.0
            delayed.append(feed_record)

    return delayed, missing

def reporter(conf_path):
    """
    Runs the main reporter loop

        conf_path (str): Path to configuration file

        returns: None
    """
    ## capture time


    ## Load conf_path params
    CONF = json.loads(open(conf_path, 'r').read())

    ## Load GTFS
    gtfs = GTFS.GTFS(CONF["gtfs_path"])
    #print(gtfs.trips.data["service_id"].unique())
    #return

    ## Check for existing logs JSON file
    try:
        LOG = Log.Log(path = "log.json")
    except FileNotFoundError:
        LOG = Log.Log()

    while True:
        NOW = datetime.datetime.now()
        ## Retrieve expected trip IDs
        curr_trips = get_current_trips(gtfs)
        #print(gtfs.trips.data[gtfs.trips.data.index.isin(curr_trips)])
        #break

        ## Query the OBA feed
        feed = OBA.query_trip_update(CONF["feed_url"], CONF["api_key"])
        live = OBA.parse_trip_updates(feed)

        ## Check for new delays
        delayed_trips, missing = check_delays_missing(gtfs, live, curr_trips,
                                                        datetime.datetime.timestamp(NOW),
                                                        delay_thresh = 5)

        delayed_trips = sorted(delayed_trips, key = lambda x: int(x["rte_id"]))
        missing = sorted(missing, key = lambda x: int(x[0]["route_id"]))

        if len(delayed_trips) > 0:
            for trip in delayed_trips:
                trip["stop_name"] = gtfs.stops.data.loc[trip["ns_id"], "stop_name"]
                trip["headsign"] = gtfs.trips.data.loc[trip.name, "trip_headsign"]

                #text = f"[{NOW.strftime("%H:%M")}] Route {trip["rte_id"]} "
                #text += f"expected at {trip["stop_name"]} is {trip["delay"]} minute(s) late"

                text = "[" + NOW.strftime("%I:%M %p") + "] " + trip["rte_id"] + "|"
                text += str(trip["delay"]) +" min : "
                text += "Route " + trip["rte_id"]
                text += " to " + trip["headsign"]
                text += " expected at " + trip["stop_name"] + " (" + str(trip["ns_id"]) + ")"
                text += " is delayed " + str(trip["delay"]) + " minutes"
                text += " (vehicle " + str(trip["veh_id"]) + ")"
                print(text+'\n')
            print("")
        else:
            print("[" + NOW.strftime("%I:%M %p") + "] No delayed routes\n")

        if len(missing) > 0:
            print("MISSING TRIPS:")
            for pair in missing:
                trip, stop = pair
                stop_name = gtfs.stops.data.loc[stop["stop_id"],:]["stop_name"]
                text = '\t'+str(trip["route_id"])+" to "+trip["trip_headsign"]
                text += " expected at "+ stop_name + " ("+stop["stop_id"]+")"
                print(text)
        print("=================\n")


        STOP = datetime.datetime.now()
        runtime = STOP - NOW
        time.sleep(120 - runtime.total_seconds())

        ## Check for resolved delays

        ## Check for untracked vehicles

        ## Check for re-tracking vehicles

        ## Sleep one minute

if __name__ == "__main__":
    print("\nGood morning! Beginning monitoring loop now...\n")
    try:
        reporter(sys.argv[1])
    except KeyboardInterrupt:
        print("\nGood night!")
