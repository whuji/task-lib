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
from dbhandler import DBHandler

class Task:
    def __init__(self, id=None, create=None, db_config=("127.0.0.1", "my_database", "my_user"), related_task_id=None, mustExit = None):
        assert id or create, "Bad arguments, no id nor create"
        assert id == None or isinstance(id, int), "Bad arguments id, must be an int"
        assert mustExit, "Bad arguments, mustExit not provided"
        assert create == None or (isinstance(create,tuple) and 3 <= len(create) <= 4), "Bad argument create, must be (user_id,type,request in json[,is_visible_to_user true/false])"
       
        self.db = DBHandler(db_config)

        if create:
            self.status, self.progress_percent, self.user_id, self.type, self.request, self.result = "created", 0, create[0], create[1], create[2], []
            if isinstance(self.request, str):
                self.request = json.loads(self.request)
            r = self.db.execute("INSERT INTO tasks (status, progress_percent, type, request, user_id) VALUES (%s, %s, %s, %s::jsonb, %s) RETURNING id, start_date, end_date ;", [self.status, self.progress_percent, self.type, json.dumps(self.request), self.user_id])
            r = self.db.fetchall(r)
            self.id, self.start_date, self.end_date = r[0]
            if related_task_id:
                self.db.execute("UPDATE tasks SET related_task_id = %s WHERE id = %s",[related_task_id, self.id])
                self.related_task_id = related_task_id
            with Notifier() as notifier:
                notifier.send_notification("task","id={},status=created".format(self.id))
        else:
            r = self.db.execute("SELECT status, progress_percent, type, request, result, user_id, start_date, end_date, related_task_id FROM tasks WHERE id = %s;", [id])
            r = self.db.fetchall(r)
            self.id = id
            self.status, self.progress_percent, self.type, self.request, self.result, self.user_id, self.start_date, self.end_date, self.related_task_id = r[0]
            if self.result == None:
                self.result = []

    def __enter__(self):
        return self

    def __del__(self):
        if self.status == "in_progress":
            self.pause()

        if self.status not in ['cancelled', 'completed']:
            self.commit()

    def __repr__(self):
        return "{}, {}, {}%, {}, {}, {}".format(self.id, self.status, self.progress_percent, self.type, self.request, self.result)

    def _setStatus(self, status, notify = True):
        self.status = status
        self.db.execute("UPDATE tasks SET status = '{}' WHERE id = {};".format(status, self.id))
        if notify:
            with Notifier() as notifier:
                notifier.sendNotification("task", "sid={},tatus={}".format(self.id, self.status))

    def _setProgressPercent(self, progress_percent, notify = True):
        self.progress_percent = progress_percent
        self.db.execute("UPDATE tasks SET progress_percent = {} WHERE id = {};".format(progress_percent, self.id))
        if notify:
           with Notifier() as notifier:
                notifier.sendNotification("task", "id={},status={},progress_percent={}".format(self.id, self.status, self.progress_percent))

    def delegate(self, delegated_task=None, delegated_type=None, delegated_req=None):
        assert delegated_task == None or isinstance(delegated_task, Task), "delegated_task must be a Task object"

        if delegated_task:
            dtask = delegated_task
        else:
            dtask = Task(create=(self.user_id, delegated_type or self.type, delegated_req or self.request, delegated_is_visible_to_users), related_task_id = self.id, db_config=self.db.db_config)

        self._setStatus("delegated")
        # Put the task read-only, the code is omitted for readability
        return dtask

    def commit(self):
        req = "UPDATE tasks SET user_id = %s, status = %s, type = %s, request = %s::jsonb, result = %s::jsonb "
        params = [self.user_id, self.status, self.type, json.dumps(self.request), json.dumps(self.result)]
        if self.end_date:
            req += ", end_date = '{}'::timestamp(0) without time zone".format(self.end_date)
        if self.progress_percent:
            req += ", progress_percent = %s"
            params.append(self.progress_percent)
        req += " WHERE id = %s;"
        params.append(self.id)
        self.db.execute(req, params)

    def update(self,progress_percent=None):
        assert progress_percent == None or (isinstance(progress_percent, int) and  0 <= progress_percent <= 100), "progress_percent must be an integer between 0 and 100"

        self._setStatus("in_progress")
        if progress_percent:
            self._setProgressPercent(progress_percent)

    def pause(self):
        if self._setStatus == "in_progress":
            self._setStatus("paused")

    def cancel(self):
        self.commit()
        self._setStatus("cancelled")

    def error(self, errorMsg = None):
        assert errorMsg is None or isinstance(errorMsg, str), "errorMsg must be a string"
        if errorMsg:
            if isinstance(self.result, list):
                self.result.append({ "error": errorMsg })
            else:
                self.result = { "error": errorMsg }
        self._setStatus("error")

    def complete(self):
        self._setProgressPercent(100, notify=False)
        self.end_date = datetime.datetime.now()
        self.commit()
        self._setStatus("completed")
