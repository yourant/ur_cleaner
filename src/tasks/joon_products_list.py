#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure

import datetime
from src.services.base_service import BaseService
import requests
from multiprocessing.pool import ThreadPool as Pool

from pymongo import MongoClient

mongo = MongoClient('192.168.0.150', 27017)
mongodb = mongo['joom']
col = mongodb['joom_productlist']



class Worker(BaseService):
    """
    worker template
    """

    def __init__(self):
        super().__init__()

    def get_joom_token(self):
        sql = 'select AccessToken, aliasName from S_JoomSyncInfo'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def clean(self):
        col.delete_many({})
        self.logger.info('success to clear joom_productlist')

    def get_order(self,row):
        token = row['AccessToken']
        suffix = row['aliasName']
        url = 'https://api-merchant.joom.com/api/v2/product/multi-get'
        headers = {'content-type': 'application/json', 'Authorization': 'Bearer ' + token}
        date = str(datetime.datetime.today() - datetime.timedelta(days=3))[:10]
        limit = 300
        start = 0
        try:
            while True:
                param = {
                    "since": date,
                    "limit": limit,
                    'start': start
                }
                response = requests.get(url, params=param, headers=headers)
                ret = response.json()
                if ret['code'] == 0 and ret['data']:
                    list = ret['data']
                    for item in list:
                        list_variants = item['Product']['variants']
                        for row in list_variants:
                            newsku = row['Variant']['sku'].split("@")[0]
                            ele = {'code': row['Variant']['sku'], 'sku': row['Variant']['sku'],
                                   'newsku': newsku, 'itemid': row['Variant']['product_id'], 'suffix': suffix,
                                   'selleruserid': '', 'storage': row['Variant']['inventory'], 'updateTime': date}
                            self.put(ele)
                            self.logger.info(f'putting {row["Variant"]["product_id"]}')
                    start += limit
                    if len(ret['data']) < limit:
                        break
                else:
                    break
        except Exception as e:
            self.logger.error(e)

    def put(self, row):
        col.save(row)

    def pull(self):
        rows = col.find()
        for row in rows:
            yield (row['code'], row['sku'], row['newsku'], row['itemid'], row['suffix'], row['selleruserid'], row['storage'], row['updateTime'])

    def save_trans(self):
        rows = self.pull()
        self.push_one(rows)
        mongo.close()

    def push_one(self, rows):
        try:
            sql = 'insert into ibay365_joom_lists(code, sku, newsku,itemid, suffix, selleruserid, storage, updateTime) values(%s,%s,%s,%s,%s,%s,%s,%s)'
            for row in rows:
                self.cur.execute(sql, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]))
            self.con.commit()
        except Exception as why:
            self.logger.error(f"fail  {why} ")

    def work(self):
        try:
            tokens = self.get_joom_token()
            self.clean()
            pl = Pool(16)
            pl.map(self.get_order, tokens)
            pl.close()
            pl.join()
            self.save_trans()
        except Exception as why:
            self.logger.error('fail to count sku cause of {} '.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


