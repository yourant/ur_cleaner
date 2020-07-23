#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure


import math
import re
import datetime
from src.services.base_service import BaseService
import requests
from multiprocessing.pool import ThreadPool as Pool

from pymongo import MongoClient

mongo = MongoClient('192.168.0.150', 27017)
mongodb = mongo['operation']
# col = mongodb['wish_products']
col = mongodb['wish_tokens']


class Worker(BaseService):
    """
    worker template
    """

    def __init__(self):
        super().__init__()

    def get_wish_token(self):
        sql = ("SELECT aliasName as suffix, AccessToken as token FROM S_WishSyncInfo WHERE  "
               "aliasname is not null"
               " and  AliasName not in "
               "(select DictionaryName from B_Dictionary where CategoryID=12 and used=1 and FitCode='Wish') "
               )
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def put(self, rows):
        for rw in rows:
            rw['updated'] = datetime.datetime.now()
            col.update_one({'suffix': rw['suffix']}, {"$set": rw}, upsert=True)

    def work(self):
        try:
            tokens = self.get_wish_token()
            self.put(tokens)
        except Exception as why:
            self.logger.error('fail to get tokens cause of {} '.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


