#!/usr/bin/env python
# -*- coding:utf-8 -*-
import psycopg2
import pymysql
import sys


def get_conn(self):
    try:
        conn = pymysql.Connect(host=self.host, port=self.port, user=self.user, passwd=self.passwd,
                               db=self.db, charset=self.charset)
        return conn
    except Exception as e:
        print('str(e):\t\t', str(e))
        sys.exit()


def get_postgresql_conn(host, port, user, passwd, db):
    try:
        conn = psycopg2.connect(host=host, port=port, user=user, password=passwd,
                                database=db)
        return conn
    except Exception as e:
        print('str(e):\t\t', str(e))
        sys.exit()
