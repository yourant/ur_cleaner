#! usr/bin/env/python3
# coding:utf-8
# Author: turpure

import json
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.services.base_service import BaseService
import requests


class Worker(BaseService):
    """
    worker
    """

    def __init__(self):
        super().__init__()

    def get_joom_token(self):
        sql = 'select AccessToken from S_JoomSyncInfo'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_order(self, token):
        token = token['AccessToken']
        url = 'https://api-merchant.joom.com/api/v2/order/multi-get'
        headers = {'content-type': 'application/json', 'Authorization': 'Bearer ' + token}
        yesterday = str(datetime.datetime.today() - datetime.timedelta(days=1))[:10]
        date = str(datetime.datetime.strptime(yesterday[:8] + '01', '%Y-%m-%d'))[:10]
        limit = 300
        try:
            for i in range(0, 1000):
                param = {
                    "since": date,
                    "limit": limit,
                    'start': i * limit
                }
                response = requests.get(url, params=param, headers=headers)
                ret = response.json()
                if ret['code'] == 0 and ret['data']:
                    orders = ret['data']
                    for order in orders:
                        try:
                            order_detail = order["Order"]
                            if order_detail['state'] == 'REFUNDED':
                                refunded = dict()
                                refunded['transaction_id'] = order_detail['transaction_id']
                                refunded['refunded_time'] = order_detail['refunded_time']
                                refunded['price'] = order_detail['price']
                                refunded['currencyCode'] = 'USD'
                                refunded['plat'] = 'joom'
                                yield refunded
                        except Exception as e:
                            self.logger.debug(e)
                    if len(ret['data']) < limit:
                        break
                else:
                    break

        except Exception as e:
            self.logger.error(e)



    def save_refund_order(self,row):
        sql = ("if not EXISTS (select id from y_refunded(nolock) where "
               "order_id=%s and refund_time= %s) "
               'insert into y_refunded(order_id, refund_time, total_value,currencyCode, plat) '
               'values(%s,%s,%s,%s,%s)'
               "else update y_refunded set "
               "total_value=%s,currencyCode=%s where order_id=%s and refund_time= %s")
        try:
            self.cur.execute(sql,
                             (row['transaction_id'], row['refunded_time'],
                              row['transaction_id'], row['refunded_time'],
                              row['price'], row['currencyCode'], row['plat'],
                              row['price'], row['currencyCode'], row['transaction_id'], row['refunded_time']))
            self.con.commit()
            self.logger.error("success to get joom refunded order!")
        except Exception as e:
            self.logger.error("failed to get joom refunded order cause of %s" % e)


    def work(self):
        try:
            tokens = self.get_joom_token()
            with ThreadPoolExecutor(16) as pool:
                future = {pool.submit(self.get_order, token): token for token in tokens}
                for fu in as_completed(future):
                    try:
                        data = fu.result()
                        for row in data:
                            self.save_refund_order(row)
                    except Exception as e:
                        self.logger.error(e)
        except Exception as why:
            self.logger.error('fail to count sku cause of {} '.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


