#!/usr/bin/env python3
# coding: utf-8
# vi: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import json
import psycopg2
import threading
import datetime
import notifier
import asyncio
import time
import traceback
from task import Task
import csv
from io import StringIO

def taskNotificationPayloadToDict(payload):
    assert isinstance(payload,str), "bad arg"
    csv_fd = StringIO(payload)
    dial = csv.Sniffer().sniff(payload)
    csv_d = csv.reader(csv_fd, dialect=dial)
    res = {}
    for line in csv_d:
        for col in line:
            if len(col) > 0:
                key,val = col.split("=",1)
                res[key] = val
    return res

class TaskManager(notifier.Notifier):
    def __init__(self, db_config=("127.0.0.1", "my_database", "my_user"), asyncio_main_loop=None, timeout=None):
        assert asyncio_main_loop == None or isinstance(asyncio_main_loop, asyncio.AbstractEventLoop), "asyncio_main_loop must be an asyncio event loop"
        super().__init__(timeout, db_config)
        self.tasks_channels = {}
        self.asyncio_main_loop = asyncio_main_loop or asyncio.get_event_loop()
        self.timeout = timeout

    def subscribeChannelCallback(self, channel, cb, sql_filter_tasks=None):
        assert sql_filter_tasks == None or isinstance(sql_filter_tasks, str), "sql_filter_tasks must be a string with a SQL filter suitable for the tasks table"
        if channel not in self.tasks_channels:
            self.tasks_channels[channel] = {}
        if sql_filter_tasks not in self.tasks_channels[channel]:
            self.tasks_channels[channel][sql_filter_tasks] = { "cb": [] }
        self.tasks_channels[channel][sql_filter_tasks]["cb"].append(cb)
        if channel not in self.channels or self._cb not in self.channels[channel]:
            super().subscribeChannelCallback(channel, self._cb)

    def _cb(self, notification):
        try:
            payload_dict = taskNotificationPayloadToDict(notification.payload)
        except Exception as e:
            raise e
            print("Error at parsing notification payload: ",e)
            return

        for task_filter in self.tasks_channels[notification.channel]:
            if task_filter:
                to_format = "SELECT id FROM tasks WHERE id = {id} AND ( "+task_filter+" );"
                res = self.db.execute(to_format.format(id=payload_dict["id"]))
                sql_res = self.db.fetchall(res)
                if len(sql_res) == 0:
                    continue

            a = self.tasks_channels[notification.channel][task_filter]
            t = Task(id=int(payload_dict["id"]), db_config=self.db.db_config, mustExit=self.mustExit)
            for cb in a["cb"]:
                if asyncio.iscoroutinefunction(cb):
                    future = asyncio.run_coroutine_threadsafe(cb(t), self.asyncio_main_loop)
                    timeout = 0
                    while timeout < self.timeout and not self.mustExit.is_set(): 
                        try: 
                            future.result(timeout=1) 
                        except asyncio.TimeoutError: 
                            print("Future timeout on asyncio callback in TaskManager notifier") 
                            timeout += self.timeout 
                    if timeout >= self.timeout:
                        future.cancel()
                else:
                    cb(t)

    def listTasks(self, status=None, type=None):
        assert status == None or isinstance(status, str), "Bad status arg"
        assert type == None or isinstance(type, str), "Bad type arg"
        sql_filter = []
        if status:
            sql_filter.append("status = {}".format(status))
        if type:
            sql_filter.append("type = '{}'".format(type))
        sql_query = "SELECT id FROM tasks "
        if len(sql_filter) > 0:
            sql_query += "WHERE " + ' AND '.join(sql_filter)
        sql_query += ";"

        tasksList = []
        res = self.db.execute(sql_query)
        sql_res = self.db.fetchall(res)
        for l in sql_res:
            for task_id in l:
                tasksList.append(Task(id=task_id, db_config=self.db.db_config, mustExit=self.mustExit))
        return tasksList
