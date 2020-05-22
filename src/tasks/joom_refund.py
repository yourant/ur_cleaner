#! usr/bin/env/python3
# coding:utf-8
# Author: turpure

import datetime
from src.services.base_service import BaseService
import requests
from multiprocessing.pool import ThreadPool as Pool
import math

from pymongo import MongoClient

mongo = MongoClient('192.168.0.150', 27017)
mongodb = mongo['joom']
col = mongodb['joom_refund']


class Worker(BaseService):
    """
    get joom refund
    """

    def __init__(self):
        super().__init__()

    def get_joom_token(self):
        sql = 'select AccessToken, aliasName from S_JoomSyncInfo'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_order(self, token_info):
        token = token_info['AccessToken']
        url = 'https://api-merchant.joom.com/api/v2/order/multi-get'
        headers = {'content-type': 'application/json', 'Authorization': 'Bearer ' + token}
        date = str(datetime.datetime.today() - datetime.timedelta(days=3))[:10]
        # yesterday = str(datetime.datetime.today() - datetime.timedelta(days=1))[:10]
        # date = str(datetime.datetime.strptime(yesterday[:8] + '01', '%Y-%m-%d'))[:10]
        limit = 300
        try:
            for i in range(0, 100000):
                param = {
                    "since": date,
                    "limit": limit,
                    'start': i * limit
                }
                response = requests.get(url, params=param, headers=headers)
                self.logger.info(f'get page {i} of {token_info["aliasName"]}')
                ret = response.json()
                if ret['code'] == 0 and ret['data']:
                    self.parse(ret['data'])
                    if len(ret['data']) < limit:
                        break
                else:
                    break

        except Exception as e:
            self.logger.error(e)

    def parse(self, rows):
        for order in rows:
            try:
                order_detail = order["Order"]
                if order_detail['state'] == 'REFUNDED':
                    ele = {'order_id': order_detail['order_id'], 'refund_time': order_detail['refunded_time'],
                                  'total_value': order_detail['order_total'], 'currencyCode': 'USD', 'plat': 'joom'}
                    self.put(ele)
                    self.logger.info(f'putting {order_detail["order_id"]}')
            except Exception as why:
              self.logger.error(f'fail to parse rows cause of {why}')

    def clean(self):
        col.delete_many({})
        self.logger.info('success to clear joom_refund')

    def put(self, row):
        col.save(row)

    def pull(self):
        rows = col.find()
        for row in rows:
            yield (row['order_id'], row['refund_time'], row['total_value'], row['currencyCode'], row['plat'])

    def push_batch(self, rows):
        try:
            rows = list(rows)
            number = len(rows)
            step = 100
            end = math.floor(number / step)
            for i in range(0, end + 1):
                value = ','.join(map(str, rows[i * step: min((i + 1) * step, number)]))
                sql = f'insert into y_refunded(order_id, refund_time, total_value,currencyCode, plat) values {value}'
                try:
                    self.cur.execute(sql)
                    self.con.commit()
                    self.logger.info(f"success to save data of joom refund from {i * step} to  {min((i + 1) * step, number)}")
                except Exception as why:
                    self.logger.error(f"fail to save data of joom refund cause of {why} ")
        except Exception as why:
            self.logger.error(f"fail to save joom refund cause of {why} ")

    def push_one(self, rows):
        try:

            sql = ("if not EXISTS (select id from y_refunded(nolock) where "
                       "order_id=%s and refund_time= %s) "
                       'insert into y_refunded(order_id, refund_time, total_value,currencyCode, plat) '
                       'values(%s,%s,%s,%s,%s)')
            for row in rows:
                self.cur.execute(sql, (row[0], row[1], row[0], row[1], row[2], row[3], row[4]))
            self.con.commit()
        except Exception as why:
            self.logger.error(f"fail to save joom refund cause of {why} ")

    def save_trans(self):
        rows = self.pull()
        self.push_one(rows)
        mongo.close()

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


