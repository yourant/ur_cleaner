#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure


from src.services.base_service import CommonService
import requests
from multiprocessing.pool import ThreadPool as Pool

from pymongo import MongoClient, errors

mongo = MongoClient('192.168.0.150', 27017)
mongodb = mongo['operation']
col = mongodb['wish_productboost_performance']
query_db = mongodb['wish_productboost']


class Worker(CommonService):
    """
    worker template
    """

    def __init__(self):
        super().__init__()
        self.tokens = dict()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    @staticmethod
    def get_wish_campaign_id():
        rows = query_db.find({}).sort("last_updated_time", -1).limit(10000)
        for row in rows:
            yield row

    def clean(self):
        col.delete_many({})
        self.logger.info('success to clear wish wish_productboost_performance list')

    def get_token(self):
        sql = f"SELECT AccessToken,aliasname FROM S_WishSyncInfo"
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        tokens = dict()
        for row in ret:
            tokens[row['aliasname']] = row['AccessToken']
        self.tokens = tokens

    def get_products(self, row):
        token = self.tokens[row['suffix']]
        campaign_id = row['_id']
        url = 'https://merchant.wish.com/api/v2/product-boost/campaign/get-performance'
        try:
            while True:
                param = {
                    "id": campaign_id,
                    'access_token': token,
                }
                ret = dict()
                for i in range(2):
                    try:
                        response = requests.get(url, params=param)
                        ret = response.json()
                        break
                    except Exception as why:
                        self.logger.error(f' fail to get of product_id of {campaign_id} '
                                          f'page cause of {why} {i} times'
                                          f'param {param} '
                                          )
                if ret and ret['code'] == 0 and ret['data']:
                    row = ret['data']['Statistics']
                    row['_id'] = campaign_id
                    row['campaign_id'] = campaign_id
                    ele = row
                    self.put(ele)
                    break
                else:
                    message = ret['message']
                    code = ret['code']
                    self.logger.error(f'fail product_id {campaign_id} code {code} message {message}')
                    break
        except Exception as e:
            self.logger.error(e)

    def put(self, row):
        col.update_one({'_id': row['_id']}, {"$set": row}, upsert=True)

    def work(self):
        try:
            self.get_token()
            product_id = self.get_wish_campaign_id()
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


