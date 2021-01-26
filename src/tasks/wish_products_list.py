#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure


import os
import re
import math
import time
import datetime
from src.services.base_service import CommonService
import requests
from multiprocessing.pool import ThreadPool as Pool

from pymongo import MongoClient

mongo = MongoClient('192.168.0.150', 27017)

operation = mongo['wish']
table = operation['wish_productlist']


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

    def get_wish_token(self):
        sql = ("SELECT AccessToken,aliasname FROM S_WishSyncInfo WHERE  "
               "aliasname is not null"
               " and  AliasName = 'WISE138-shelleily' "
               " and  AliasName not in "
               "(select DictionaryName from B_Dictionary where CategoryID=12 and used=1 and FitCode='Wish') ")
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_data(self, row):
        # print(row)
        token = row['AccessToken']
        suffix = row['aliasname']
        url = 'https://merchant.wish.com/api/v2/product/multi-get'
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
                    # 'show_rejected':'true',
                    # 'since': since
                }
                ret = dict()
                for i in range(2):
                    try:
                        response = requests.get(url, params=param)
                        ret = response.json()
                        # print(ret)
                        break
                    except Exception as why:
                        self.logger.error(f' fail to get of products of {suffix} in {start}  '
                                          f'page cause of {why} {i} times '
                                          f'param {param} ')
                if ret and ret['code'] == 0 and ret['data']:
                    pro_list = ret['data']
                    for item in pro_list:
                        ele = item['Product']
                        ele['_id'] = ele['id']
                        ele['suffix'] = suffix
                        try:
                            table.insert_one(ele)
                        except Exception as why:
                            self.logger.error(f" fail to insert {ele['id']} cause of {why}")
                        # self.logger.info(f'putting {ele["_id"]}')
                    if 'next' in ret['paging']:
                        arr = ret['paging']['next'].split("&")[1]
                        start = re.findall("\d+", arr)[0]
                    else:
                        break
                else:
                    break
        except Exception as e:
            self.logger.error(e)

    @staticmethod
    def pull():
        # rows = col.find({'sku':{'$regex':"8C1085"}})
        # rows = table.find()
        rows = table.find({"removed_by_merchant": "False", "review_status": "approved"})
        for row in rows:
            yield (row['code'], row['sku'], row['newSku'], row['itemid'], row['suffix'], row['selleruserid'],
                   row['storage'], row['listingType'], row['country'], row['paypal'], row['site'], row['updateTime'])

    @staticmethod
    def get_products():
        rows = table.find({"removed_by_merchant": "False", "review_status": "approved"})
        for rw in rows:
            for row in rw['variants']:
                new_sku = row['Variant']['sku'].split("@")[0]
                ele = {'code': row['Variant']['sku'], 'sku': row['Variant']['sku'],
                       'newsku': new_sku, 'itemid': row['Variant']['product_id'], 'suffix': rw['suffix'],
                       'selleruserid': '', 'storage': row['Variant']['inventory'], 'updateTime': str(datetime.datetime.today())[:19],
                       'enabled': row['Variant']['enabled'], 'removed_by_merchant': rw['removed_by_merchant']}
                ele['_id'] = ele['itemid']
                yield (ele['code'], ele['sku'], ele['newsku'], ele['itemid'], ele['suffix'], ele['selleruserid'],
                       ele['storage'], ele['updateTime'])

    def clear_db(self):
        table.delete_many({})
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
        # rows = self.get_products()
        # self.push_db(rows)
        # rows = self.pull()
        rows = self.get_products()
        self.push_db(rows)
        mongo.close()

    def work(self):
        begin = time.time()
        try:
            self.clear_db()
            tokens = self.get_wish_token()
            pl = Pool(50)
            pl.map(self.get_data, tokens)
            pl.close()
            pl.join()
            self.save_trans()

        except Exception as why:
            self.logger.error(why)
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()
            mongo.close()
        print('程序耗时{:.2f}'.format(time.time() - begin))  # 计算程序总耗时


if __name__ == "__main__":
    worker = Sync()
    worker.work()


