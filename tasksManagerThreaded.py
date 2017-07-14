#!/usr/bin/env python3
# coding: utf-8
# vi: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import threading
import tasksManager

class TaskManagerThreaded(tasksManager.TaskManager, threading.Thread):
    def __init__(self, db_config=("127.0.0.1", "my_database", "my_user"), asyncio_main_loop=None, timeout=2):
        threading.Thread.__init__(self)
        tasksManager.TaskManager.__init__(self, db_config, asyncio_main_loop, timeout)
