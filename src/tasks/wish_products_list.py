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
mongodb = mongo['wish']
col = mongodb['wish_productlist']


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
        url = 'https://merchant.wish.com/api/v2/product/multi-get'
        # headers = {'content-type': 'application/json', 'Authorization': 'Bearer ' + token}
        date = str(datetime.datetime.today() - datetime.timedelta(days=0))[:10]
        since = str(datetime.datetime.today() - datetime.timedelta(days=30))[:10]
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
                        list_variants = item['Product']['variants']
                        list_removed_by_merchant = item['Product']['removed_by_merchant']
                        for row in list_variants:
                            new_sku = row['Variant']['sku'].split("@")[0]
                            ele = {'code': row['Variant']['sku'], 'sku': row['Variant']['sku'],
                                   'newsku': new_sku, 'itemid': row['Variant']['product_id'], 'suffix': suffix,
                                   'selleruserid': '', 'storage': row['Variant']['inventory'], 'updateTime': date,
                                   'enabled': row['Variant']['enabled'], 'removed_by_merchant': list_removed_by_merchant}
                            ele['_id'] = ele['itemid']
                            self.put(ele)
                            # self.logger.info(f'putting {row["Variant"]["product_id"]}')
                    start += limit
                    if len(ret['data']) < limit:
                        break
                else:
                    break
        except Exception as e:
            self.logger.error(e)

    def put(self, row):
        col.update_one({'_id': row['_id']}, {"$set": row}, upsert=True)

    def pull(self):
        rows = col.find({"removed_by_merchant": "False"})
        for row in rows:
            yield (row['code'], row['sku'], row['newsku'], row['itemid'], row['suffix'], row['selleruserid'], row['storage'], row['updateTime'])

    def save_trans(self):
        self.clear_db()
        rows = self.pull()
        # self.push_one(rows)
        self.push_batch(rows)

    def clear_db(self):
        sql = 'truncate table ibay365_wish_lists'
        self.cur.execute(sql)
        self.con.commit()
        self.logger.info('success to clear wish lists')

    def push_one(self, rows):
        try:
            sql = 'insert into ibay365_wish_lists(code, sku, newsku,itemid, suffix, selleruserid, storage, updateTime) values(%s,%s,%s,%s,%s,%s,%s,%s)'
            for row in rows:
                self.cur.execute(sql, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7]))
            self.con.commit()
        except Exception as why:

            self.logger.error(f"fail  {why} ")

    def push_batch(self, rows):
        try:
            rows = list(rows)
            number = len(rows)
            step = 100
            end = math.ceil(number / step)
            for i in range(0, end):
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

    def work(self):
        try:
            tokens = self.get_wish_token()
            self.clean()
            pl = Pool(16)
            pl.map(self.get_products, tokens)
            pl.close()
            pl.join()
            self.save_trans()
        except Exception as why:
            self.logger.error('fail to count sku cause of {} '.format(why))
        finally:
            self.close()
            mongo.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


