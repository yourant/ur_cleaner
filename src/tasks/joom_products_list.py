#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure


import os
import math
import re
import datetime
from src.services.base_service import CommonService
import requests
from multiprocessing.pool import ThreadPool as Pool

from pymongo import MongoClient

mongo = MongoClient('192.168.0.150', 27017)
mongodb = mongo['joom']
col = mongodb['joom_productlist']


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
        since = str(datetime.datetime.today() - datetime.timedelta(days=5))[:10]
        limit = 300
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
                        list_variants = item['Product']['variants']
                        list_enabled = item['Product']['enabled']
                        list_state = item['Product']['state']
                        for row in list_variants:
                            new_sku = row['Variant']['sku'].split("@")[0]
                            ele = {'_id': row['Variant']['sku'],'code': row['Variant']['sku'], 'sku': row['Variant']['sku'],
                                   'newsku': new_sku, 'itemid': row['Variant']['product_id'], 'suffix': suffix,
                                   'selleruserid': '', 'storage': row['Variant']['inventory'], 'updateTime': date,
                                   'enabled': list_enabled, 'state': list_state}
                            self.put(ele)
                            # self.logger.info(f'putting {row["Variant"]["product_id"]}')
                    if 'paging' in ret and 'next' in ret['paging']:
                        arr = ret['paging']['next'].split("&")[2]
                        start = re.findall("\d+", arr)[0]
                    else:
                        break
                    # start += limit
                    # if len(ret['data']) < limit:
                    #     break
                else:
                    break
        except Exception as e:
            self.logger.error(e)

    def put(self, row):
        col.update_one({'_id': row['_id']}, {"$set": row}, upsert=True)
        # col.save(row)

    def pull(self):
        rows = col.find({"enabled": "True","state":{'$nin':['rejected']}})
        for row in rows:
            yield (row['code'], row['sku'], row['newsku'], row['itemid'], row['suffix'], row['selleruserid'], row['storage'], row['updateTime'])

    def save_trans(self):
        rows = self.pull()
        self.push_one(rows)
        # self.push_batch(rows)
        mongo.close()

    def push_one(self, rows):
        try:
            sql = ("if not EXISTS (select id from ibay365_joom_lists(nolock) where "
                   "code=%s and itemid=%s) "
                   "insert into ibay365_joom_lists (code, sku, newsku,itemid, suffix, selleruserid, storage, updateTime) "
                   "values (%s,%s,%s,%s,%s,%s,%s,%s) "
                   "else update ibay365_joom_lists set "
                   "storage=%s,updateTime=%s where code=%s and itemid=%s")
            for row in rows:
                self.cur.execute(sql, (
                row[0], row[3], row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[6], datetime.datetime.now(), row[0],
                row[3]))
                # self.logger.info(f'putting {row[2]}')
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
                sql = f'insert into ibay365_joom_lists(code, sku, newsku,itemid, suffix, selleruserid, storage, updateTime) values {value}'
                try:
                    self.cur.execute(sql)
                    self.con.commit()
                    # self.logger.info(f"success to save data of joom products from {i * step} to  {min((i + 1) * step, number)}")
                except Exception as why:
                    self.logger.error(f"fail to save data of joom products cause of {why} ")
        except Exception as why:
            self.logger.error(f"fail to save joom products cause of {why} ")

    def work(self):
        try:
            tokens = self.get_joom_token()
            self.clean()
            pl = Pool(16)
            pl.map(self.get_products, tokens)
            pl.close()
            pl.join()
            self.save_trans()
        except Exception as why:
            self.logger.error('fail to count sku cause of {} '.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


