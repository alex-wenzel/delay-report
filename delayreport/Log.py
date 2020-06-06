"""
Class that keeps track of the current delayed/missing buses
"""

import binascii
import os

class Log:
    def __init__(self, path = None):
        """
        Initialization

            path (str|None): Existing log to load, if None, make a new log

            returns: None
        """
        self.path = path

        if self.path == None:
            self.log = {
                "active_delays": {},
                "active_dropoffs": {}
            }
        else:
            self.log = ""

    def add_active_delay(
        self,
        trip_id,
        route_id,
        ns_id,
        ns_pt,
        delay_len
    ):
        """
        Add a new delay
        """
        delay_id = binascii.b2a_hex(os.urandom(15))
