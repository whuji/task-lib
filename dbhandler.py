#!/usr/bin/env python3
# coding: utf-8
# vi: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import sys
import psycopg2

class DBHandler():
    def __init__(self, db_config=("127.0.0.1", "my_database", "my_user")):
        assert isinstance(db_config, tuple), "db_config must be a tuple (db_host, db_name, db_user)"
        self.db_config = db_config
        self.db_conn = psycopg2.connect(host=db_config[0], database=db_config[1], user=db_config[2])
        self.db_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def execute(self, req, params=None):
        assert isinstance(req, str), "req must be string"
        assert params == None or isinstance(params, list), "params must be []"
        cursor = self.db_conn.cursor()
        cursor.execute(req, params)
        return cursor

    def fetchall(self, res):
        assert isinstance(res, psycopg2.extensions.cursor), "argument must be the object returned by execute"
        return res.fetchall()
