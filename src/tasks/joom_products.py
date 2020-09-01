#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure


import math
import re
import datetime
from src.services.base_service import CommonService
import requests
from multiprocessing.pool import ThreadPool as Pool

from pymongo import MongoClient

mongo = MongoClient('192.168.0.150', 27017)
mongodb = mongo['operation']
col = mongodb['joom_products']


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

    def get_joom_token(self):
        sql = 'select AccessToken, aliasName from S_JoomSyncInfo'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def clean(self):
        col.delete_many({})
        self.logger.info('success to clear joom product list')

    def get_products(self, row):
        token = row['AccessToken']
        suffix = row['aliasName']
        url = 'https://api-merchant.joom.com/api/v2/product/multi-get'
        # headers = {'content-type': 'application/json', 'Authorization': 'Bearer ' + token}
        date = str(datetime.datetime.today() - datetime.timedelta(days=0))[:10]
        since = str(datetime.datetime.today() - datetime.timedelta(days=7))[:10]
        limit = 200
        start = 0
        try:
            while True:
                param = {
                    "limit": limit,
                    'start': start,
                    'since': since,
                    'access_token': token,
                }
                ret = dict()
                for i in range(2):
                    try:
                        response = requests.get(url, params=param, timeout=20)
                        ret = response.json()
                        break
                    except Exception as why:
                        self.logger.error(f' fail to get of products of {suffix} in {start}  '
                                          f'page cause of {why} {i} times  '
                                          f'param {param} '
                                          )
                if ret['code'] == 0 and ret['data']:
                    listing = ret['data']
                    for item in listing:
                        ele = item['Product']
                        ele['_id'] = ele['id']
                        self.put(ele)
                        # self.logger.info(f'putting {row["Variant"]["product_id"]}')
                    if 'paging' in ret and 'next' in ret['paging']:
                        arr = ret['paging']['next'].split("&")[2]
                        start = re.findall("\d+", arr)[0]
                    else:
                        break
                else:
                    break
        except Exception as e:
            self.logger.error(e)

    def put(self, row):
        col.update_one({'_id': row['_id']}, {"$set": row}, upsert=True)
        # col.save(row)


    def work(self):
        try:
            tokens = self.get_joom_token()
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


