#!/usr/bin/env python3
# coding: utf-8
# vi: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import selectors
import psycopg2
import psycopg2.extensions
import threading
from dbhandler import DBHandler

class Notifier:
    def __init__(self, timeout = None, db_config=("127.0.0.1", "my_database", "my_user")):
        assert timeout == None or isinstance(timeout, int), "timeout must be an integer in seconds"
        self.channels = {} # channel_name = [channel_callback]
        self.mustExit = threading.Event()
        self.timeout = timeout
        self.db = DBHandler(db_config)
    
    def __enter__(self):
        return self

    def __exit__(self):
        self.stop()
        
    def __del__(self):
        for channel in self.channels.keys():
            self.db.execute("UNLISTEN {}".format(channel))
    
    def __repr__(self):
        return "subscribed channels: {}".format(self.channels.keys())
    
    def stop(self):
        self.mustExit.set()
        
    def subscribeChannelCallback(self, channel, cb):
        """ Subscribe a callback to a channel. If events are received on this channel, the callback will be triggered """
        if channel not in self.channels:
            self.channels[channel] = []
            self.db.execute("LISTEN {};".format(channel))
        self.channels[channel].append(cb)

    def sendNotification(self, channel, msg):
        self.db.execute("NOTIFY task,'{}';".format(msg))

    def run(self, timeout=None):
        assert timeout == None or is_instance(timeout,int), "timeout must be integer in seconds"
        if not timeout:
            timeout = self.timeout
        sel = selectors.DefaultSelector()
        sel.register(self.db.db_conn, selectors.EVENT_READ)
        while not self.mustExit.is_set():
            events = sel.select(timeout)
            if len(events) == 0:
                continue
            for key, mask in events:
                if key.fileobj == self.db.db_conn:
                    self.db.db_conn.poll()
                    for notification in self.db.db_conn.notifies:
                        if notification.channel in self.channels:
                            for action in self.channels[notification.channel]:
                                if action:
                                    action(notification)
                    self.db.db_conn.notifies.clear()

