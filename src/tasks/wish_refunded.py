#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-20 10:02
# Author: turpure

import json
import datetime
import requests
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.pool import ThreadPool as Pool
from tenacity import retry, stop_after_attempt
from src.services import db, log


class WishRefund(object):
    """
    get refunded orders of wish
    """
    def __init__(self):
        self.con = db.Mssql().connection
        self.logger = log.SysLogger().log
        self.cur = self.con.cursor(as_dict=True)

    def run_sql(self, sql):
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_wish_token(self):
        sql = "SELECT AccessToken,aliasname FROM S_WishSyncInfo WHERE  " \
              "datediff(DAY,LastSyncTime,getdate())<5 and aliasname is not null"
        tokens = self.run_sql(sql)
        return tokens

    @retry(stop=stop_after_attempt(3))
    def get_wish_orders(self, token):
        date = str(datetime.datetime.now() - datetime.timedelta(days=2))[:10]
        url = "https://china-merchant.wish.com/api/v2/order/multi-get"
        start = 0
        try:
            while True:
                data = {
                    "access_token": token['AccessToken'],
                    "format": "json",
                    "start": start,
                    "limit": 500,
                    "since": date,
                }
                r = requests.get(url, params=data)
                res_dict = json.loads(r.content)
                orders = res_dict['data']
                for order in orders:
                    try:
                        order_detail = order["Order"]
                        if "REFUNDED" in order_detail['refunded_by']:
                            order_detail['aliasname'] = token['aliasname']
                            yield order_detail
                    except Exception as e:
                        self.logger.debug(e)
                order_number = len(orders)
                if order_number >= 500:
                    start += 500
                else:
                    break
        except Exception as e:
            self.logger.error(e)
            raise Exception(e)

    def save_data(self, row):
        sql = "INSERT INTO Y_wish_refunded (order_id,refund_time,total_value,notename)" \
              " VALUES(%s,%s,%s,%s)"
        try:
            self.cur.execute(sql, (row['order_id'], row['last_updated'], row['order_total'], row['aliasname']))
            self.con.commit()
            self.logger.info('save %s' % row['order_id'])
        except Exception as e:
            self.logger.error('fail to save %s cause of %s' % (row['order_id'], e))

    def save_trans(self, token):
        orders = self.get_wish_orders(token)
        try:
            for row in orders:
                self.save_data(row)
        except Exception as e:
            self.logger.error(e)

    def run(self):
        try:
            tokens = self.get_wish_token()
            pool = ThreadPoolExecutor()
            ret = pool.map(self.get_wish_orders, tokens)
            for order in ret:
                for row in order:
                    self.save_data(row)
        except Exception as e:
            self.logger.error(e)


if __name__ == "__main__":
    worker = WishRefund()
    worker.run()




