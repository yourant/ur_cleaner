#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-30 15:09
# Author: turpure

from dao.base_dao import BaseDao
from src.services import log, db
import pymysql
from pymongo import MongoClient, ReadPreference
from configs.config import Config
config = Config()


class CommonService(object):
    """
    wrap log and db service
    """
    base_dao = BaseDao()

    def __init__(self):
        self.logger = self.base_dao.logger
        self.mongo = self.connect_mongo()

    @staticmethod
    def connect_mongo():
        mongo_config = config.get_config('mongo')
        client = MongoClient(mongo_config['url'].split(','))
        db_auth = client.admin
        db_auth.authenticate(mongo_config['username'], mongo_config['password'])
        return client

    def get_mongo_collection(self, database, collection):
        client = self.mongo
        db = client.get_database(database, read_preference=ReadPreference.SECONDARY_PREFERRED)
        collection = db.get_collection(collection)
        return collection

    def close_mongo(self):
        self.mongo.close()

    def close(self):
        self.close_mongo()


class BaseService(object):
    """
    wrap log and db service
    """
    def __init__(self):
        self.logger = log.SysLogger().log
        self.mssql = db.DataBase('mssql')
        self.con = self.mssql.connection
        if self.con:
            self.cur = self.con.cursor(as_dict=True)
        self.mysql = db.DataBase('mysql')
        self.warehouse_con = self.mysql.connection
        if self.warehouse_con:
            self.warehouse_cur = self.warehouse_con.cursor(pymysql.cursors.DictCursor)

        erp = config.get_config('erp')
        if erp:
            self.erp = db.DataBase('erp')
            self.erp_con = self.erp.connection
            if self.erp_con:
                self.erp_cur = self.erp_con.cursor(pymysql.cursors.DictCursor)

        self.ibay = db.DataBase('ibay')
        self.ibay_con = self.ibay.connection
        if self.ibay_con:
            self.ibay_con.set_client_encoding('utf8')
            self.ibay_cur = self.ibay_con.cursor()

    def close(self):
        try:
            # self.cur.close()
            # self.con.close()
            # self.warehouse_cur.close()
            # self.warehouse_con.close()
            self.mysql.close()
            self.mssql.close()
            erp = config.get_config('erp')
            if erp:
                self.erp.close()
            ibay = config.get_config('ibay')
            if ibay:
                self.ibay.close()

            self.logger.info('close connection')
        except Exception as e:
            self.logger.error('fail to close connection cause of {}'.format(e))



