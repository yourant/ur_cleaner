#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure


import os
import datetime
from src.services.base_service import CommonService


class Worker(CommonService):
    """
    worker template
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.col = self.get_mongo_collection('operation', 'wish_tokens')

    def close(self):
        super().close()
        self.base_dao.close_cur(self.cur)

    def get_wish_token(self):
        sql = ("SELECT aliasName as suffix, AccessToken as token FROM S_WishSyncInfo(nolock) WHERE  "
               "aliasname is not null"
               " and  AliasName not in "
               "(select DictionaryName from B_Dictionary(nolock) where CategoryID=12 and used=1 and FitCode='Wish') "
               )
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def put(self, rows):
        for rw in rows:
            rw['updated'] = datetime.datetime.now()
            self.col.update_one({'suffix': rw['suffix']}, {"$set": rw}, upsert=True)

    def work(self):
        try:
            tokens = self.get_wish_token()
            self.put(tokens)
            self.logger.info('success to get tokens of wish')
        except Exception as why:
            self.logger.error('fail to get tokens cause of {} '.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


