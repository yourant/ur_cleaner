#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-30 15:09
# Author: turpure

from src.services import log, db
import pymysql


class BaseService(object):
    """
    wrap log and db service
    """
    def __init__(self):
        self.logger = log.SysLogger().log
        self.con = db.DataBase('mssql').connection
        self.cur = self.con.cursor(as_dict=True)
        self.warehouse_con = db.DataBase('mysql').connection
        self.warehouse_cur = self.warehouse_con.cursor(pymysql.cursors.DictCursor)

    def close(self):
        try:
            self.cur.close()
            self.con.close()
            self.warehouse_cur.close()
            self.warehouse_con.close()
            self.logger.info('close connection')
        except Exception as e:
            self.logger.error('fail to close connection cause of {}'.format(e))


