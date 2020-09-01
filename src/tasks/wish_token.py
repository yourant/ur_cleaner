#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure


import datetime
from src.services.base_service import CommonService

from pymongo import MongoClient

mongo = MongoClient('192.168.0.150', 27017)
mongodb = mongo['operation']
# col = mongodb['wish_products']
col = mongodb['wish_tokens']


class Worker(CommonService):
    """
    worker template
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

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
            self.logger.error('success to get tokens of wish')
        except Exception as why:
            self.logger.error('fail to get tokens cause of {} '.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


