#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure


import math
import datetime
from src.services.base_service import BaseService
import requests
from multiprocessing.pool import ThreadPool as Pool

from pymongo import MongoClient

mongo = MongoClient('192.168.0.150', 27017)
mongodb = mongo['operation']
col = mongodb['wish_products']


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
        self.logger.info('success to clear wish product list')

    def get_products(self, row):
        token = row['AccessToken']
        suffix = row['aliasname']
        url = 'https://merchant.wish.com/api/v2/product/add'
        # headers = {'content-type': 'application/json', 'Authorization': 'Bearer ' + token}
        date = str(datetime.datetime.today() - datetime.timedelta(days=0))[:10]
        since = str(datetime.datetime.today() - datetime.timedelta(days=5))[:10]
        limit = 250
        start = 0
        try:
            while True:
                param = {
                    "limit": limit,
                    'start': start,
                    'access_token': token,
                    'since': since
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
                        ele = item['Product']
                        ele['_id'] = ele['id']
                        if 'default_shipping_price' in ele:
                            ele['default_shipping_price'] = float(ele['default_shipping_price'])
                        else:
                            ele['default_shipping_price'] = 0
                        if 'max_quantity' in ele:
                            ele['max_quantity'] = int(ele['max_quantity'])
                        else:
                            ele['max_quantity'] = 0
                        if 'localized_default_shipping_price' in ele:
                            ele['localized_default_shipping_price'] = float(ele['localized_default_shipping_price'])
                        else:
                            ele['localized_default_shipping_price'] = 0
                        ele['date_uploaded'] = datetime.datetime.strptime(ele['date_uploaded'], "%m-%d-%Y")
                        ele['last_updated'] = datetime.datetime.strptime(ele['last_updated'], "%m-%d-%YT%H:%M:%S")
                        ele['number_saves'] = int(ele['number_saves'])
                        ele['number_sold'] = int(ele['number_sold'])
                        ele['suffix'] = suffix
                        self.put(ele)
                        self.logger.info(f'putting {ele["_id"]}')
                    start += limit
                    if len(ret['data']) < limit:
                        break
                else:
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
            self.logger.error('fail to count sku cause of {} '.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


