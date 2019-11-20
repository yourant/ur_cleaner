#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-11-20 15:41
# Author: turpure


from src.services.base_service import BaseService
from pymongo import MongoClient


class Worker(BaseService):

    def __init__(self):
        super().__init__()
        self.mongo = MongoClient('192.168.0.150', 27017)
        self.mongodb = self.mongo['product_engine']

    def get_stores(self):
        sql = 'SELECT distinct eBayUserID,NoteName FROM S_PalSyncInfo'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        return ret

    def run(self):

        try:
            stores = self.get_stores()
            col = self.mongodb['ebay_stores']
            col.insert_many(stores)
            self.logger.info(f'success to sync ebay stores')

        except Exception as why:
            self.logger.error(f'fail to sync ebay stores cause of {why}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.run()
