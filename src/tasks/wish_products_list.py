#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure


import os
import math
import datetime
from src.services.base_service import CommonService
import requests
from multiprocessing.pool import ThreadPool as Pool

from pymongo import MongoClient

mongo = MongoClient('192.168.0.150', 27017)

operation = mongo['operation']
table = operation['wish_products']


class Sync(CommonService):
    """
    sync
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    @staticmethod
    def get_products():
        rows = table.find({"removed_by_merchant": "False", "review_status": "approved"})
        for rw in rows:
            for row in rw['variants']:
                new_sku = row['Variant']['sku'].split("@")[0]
                ele = {'code': row['Variant']['sku'], 'sku': row['Variant']['sku'],
                       'newsku': new_sku, 'itemid': row['Variant']['product_id'], 'suffix': rw['suffix'],
                       'selleruserid': '', 'storage': row['Variant']['inventory'], 'updateTime': str(datetime.datetime.today())[:10],
                       'enabled': row['Variant']['enabled'], 'removed_by_merchant': rw['removed_by_merchant']}
                ele['_id'] = ele['itemid']
                yield (ele['code'], ele['sku'], ele['newsku'], ele['itemid'], ele['suffix'], ele['selleruserid'], ele['storage'], ele['updateTime'])

    def clear_db(self):
        sql = 'truncate table ibay365_wish_lists'
        self.cur.execute(sql)
        self.con.commit()
        self.logger.info('success to clear wish lists')

    def push_db(self, rows):
        try:
            rows = list(rows)
            number = len(rows)
            step = 100
            end = math.ceil(number / step)
            for i in range(0, end):
                print(i)
                value = ','.join(map(str, rows[i * step: min((i + 1) * step, number)]))
                sql = f'insert into ibay365_wish_lists(code, sku, newsku,itemid, suffix, selleruserid, storage, updateTime) values {value}'
                try:
                    self.cur.execute(sql)
                    self.con.commit()
                    self.logger.info(f"success to save data of wish products from {i * step} to  {min((i + 1) * step, number)}")
                except Exception as why:
                    self.logger.error(f"fail to save data of wish products cause of {why} ")
        except Exception as why:
            self.logger.error(f"fail to save wish products cause of {why} ")

    def save_trans(self):
        self.clear_db()
        rows = self.get_products()
        self.push_db(rows)

    def work(self):
        try:
            self.save_trans()
        except Exception as why:
            self.logger.error('fail to count sku cause of {} '.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()
            mongo.close()


if __name__ == "__main__":
    worker = Sync()
    worker.work()


