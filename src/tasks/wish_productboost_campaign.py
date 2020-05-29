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
col = mongodb['wish_productboost']


class Worker(BaseService):
    """
    worker template
    """

    def __init__(self):
        super().__init__()

    def get_wish_token(self):
        sql = ("SELECT AccessToken,aliasname FROM S_WishSyncInfo WHERE  "
               "aliasname is not null"
               " and  AliasName not in "
               "(select DictionaryName from B_Dictionary where CategoryID=12 and used=1 and FitCode='Wish') "
               )
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def clean(self):
        col.delete_many({})
        self.logger.info('success to clear wish wish_productboost list')

    def get_products(self, row):
        token = row['AccessToken']
        suffix = row['aliasname']
        url = 'https://merchant.wish.com/api/v2/product-boost/campaign/multi-get'
        # headers = {'content-type': 'application/json', 'Authorization': 'Bearer ' + token}
        date = str(datetime.datetime.today() - datetime.timedelta(days=0))[:10]
        since = str(datetime.datetime.today() - datetime.timedelta(days=3))[:10]
        limit = 500
        start = 0
        try:
            while True:
                param = {
                    "limit": limit,
                    'start': start,
                    'access_token': token,
                }
                ret = dict()
                for i in range(2):
                    try:
                        response = requests.get(url, params=param)
                        ret = response.json()
                        break
                    except Exception as why:
                        self.logger.error(f' fail to get of products of {suffix} in {start}  '
                                          f'page cause of {why} {i} times'
                                          f'param {param} '
                                          )

                if ret and ret['code'] == 0 and ret['data']:
                    list = ret['data']
                    for item in list:
                        ele = item['Campaign']
                        ele['_id'] = ele['campaign_id']
                        ele['suffix'] = suffix
                        self.put(ele)
                    start += limit
                    if len(ret['data']) < limit:
                        break
                else:
                    message = ret['message']
                    code = ret['code']
                    self.logger.error(f'fail products {suffix} in {start} code {code} message {message}')
                    break
        except Exception as e:
            self.logger.error(e)

    def put(self, row):
        col.update_one({'_id': row['_id']}, {"$set": row}, upsert=True)

    def work(self):
        try:
            tokens = self.get_wish_token()
            # self.clean()
            pl = Pool(16)
            pl.map(self.get_products, tokens)
            pl.close()
            pl.join()
        except Exception as why:
            self.logger.error('fail to get wish campaign list  cause of {} '.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


