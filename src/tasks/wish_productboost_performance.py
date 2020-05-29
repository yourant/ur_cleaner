#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure


import math
import datetime
from src.services.base_service import BaseService
import requests
from multiprocessing.pool import ThreadPool as Pool

from pymongo import MongoClient, errors

mongo = MongoClient('192.168.0.150', 27017)
mongodb = mongo['operation']
col = mongodb['wish_productboost_performance']
querydb = mongodb['wish_productboost']


class Worker(BaseService):
    """
    worker template
    """

    def __init__(self):
        super().__init__()

    def get_wish_product_id(self):
        rows = querydb.find({}).limit(1)
        for row in rows:
            yield row

    def clean(self):
        col.delete_many({})
        self.logger.info('success to clear wish wish_productboost list')

    def get_token(self,row):
        suffix = row['suffix']
        sql = f"select token from ibay365_wish_quantity where suffix='{suffix}'"
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        return ret[0]['token']

    def get_products(self, row):
        token = self.get_token(row)
        product_id = row['_id']
        url = 'https://merchant.wish.com/api/v2/product-boost/campaign/get-performance'
        try:
            while True:
                param = {
                    "id": product_id,
                    'access_token': token,
                }
                ret = dict()
                for i in range(2):
                    try:
                        response = requests.get(url, params=param)
                        ret = response.json()
                        break
                    except Exception as why:
                        self.logger.error(f' fail to get of product_id of {product_id} '
                                          f'page cause of {why} {i} times'
                                          f'param {param} '
                                          )
                if ret and ret['code'] == 0 and ret['data']:
                    list = ret['data']['Statistics']
                    list['_id'] = product_id
                    ele = list
                    self.put(ele)
                    break
                else:
                    message = ret['message']
                    code = ret['code']
                    self.logger.error(f'fail product_id {product_id} code {code} message {message}')
                    break
        except Exception as e:
            self.logger.error(e)

    def put(self, row):
        col.update_one({'_id': row['_id']}, {"$set": row}, upsert=True)

    def work(self):
        try:
            # self.get_wish_product_id()
            product_id = self.get_wish_product_id()
            print(product_id)
            self.clean()
            pl = Pool(16)
            pl.map(self.get_products, product_id)
            pl.close()
            pl.join()
        except Exception as why:
            self.logger.error('fail to get wish campaign list  cause of {} '.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


