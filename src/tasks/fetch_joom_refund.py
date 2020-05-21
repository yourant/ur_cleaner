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

    def get_joom_refund_order(self):
        tokens = self.get_joom_token()
        base_url = 'https://api-merchant.joom.com/api/v2/order/multi-get'
        for row in tokens:
            self.get_order(row, base_url)

    def get_order(self, row, base_url):
        token = row['AccessToken']
        date = str(datetime.datetime.now() - datetime.timedelta(days=3))[:10]
        headers = {'content-type': 'application/json', 'Authorization': 'Bearer ' + token}
        base_url = base_url
        start = 0
        data = {
            "start": start,
            "limit": 300,
            "since": date,
        }
        try:
            for i in range(2):
                try:
                    if base_url == 'https://api-merchant.joom.com/api/v2/order/multi-get':
                        ret = requests.get(base_url, params=data, headers=headers)
                    else:
                        ret = requests.get(base_url, headers=headers)
                    break
                except Exception as why:
                    self.logger.error(f'retrying {i} times')

            res_dict = json.loads(ret.content)
            orders = res_dict['data']
            for order in orders:
                try:
                    order_detail = order["Order"]
                    if 'refunded_time' in order_detail:
                        refunded = dict()
                        refunded['buyer_id'] = order_detail['buyer_id']
                        refunded['refunded_time'] = order_detail['refunded_time']
                        refunded['price'] = order_detail['price']
                        refunded['plat'] = 'joom'
                        yield refunded
                except Exception as e:
                    self.logger.debug(e)
            paging = res_dict.get('paging', None)
            if not paging is None:
                if next or paging:
                    url = paging['next']
                    self.get_order(row, url)

        except Exception as e:
            self.logger.debug(e)

    def save_refund_order(self,row):
        sql = ("if not EXISTS (select id from y_refunded_joom_test(nolock) where "
               "order_id=%s and refund_time= %s) "
               'insert into y_refunded_joom_test(order_id, refund_time, total_value, plat) '
               'values(%s,%s,%s,%s)'
               "else update y_refunded_joom_test set "
               "total_value=%s where order_id=%s and refund_time= %s")
        try:
            self.cur.execute(sql,
                             (row['buyer_id'], row['refunded_time'],
                              row['buyer_id'], row['refunded_time'],
                              row['price'], row['plat'],
                              row['price'], row['buyer_id'], row['refunded_time']))
            self.con.commit()
            self.logger.info("success to get joom refunded order!")
        except Exception as e:
            self.logger.error("failed to get joom refunded order cause of %s" % e)

    def work(self):
        try:
            tokens = self.get_joom_token()
            base_url = 'https://api-merchant.joom.com/api/v2/order/multi-get'
            with ThreadPoolExecutor(16) as pool:
                future = {pool.submit(self.get_order, token, base_url): token for token in tokens}
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


