#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-20 10:02
# Author: turpure

import os
import json
import datetime
import requests
from concurrent.futures import ThreadPoolExecutor
from src.services.base_service import CommonService


class WishRefund(CommonService):
    """
    get refunded orders of wish
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.col = self.get_mongo_collection('wish', 'wish_refunded')

    def close(self):
        self.base_dao.close_cur(self.cur)

    def run_sql(self, sql):
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_wish_token(self):
        sql = ("SELECT AccessToken,aliasname FROM S_WishSyncInfo(nolock) WHERE  " 
              # "  datediff(DAY,LastSyncTime,getdate())<5 and "
               "aliasname is not null"
                " and  AliasName not in "
               "(select DictionaryName from B_Dictionary(nolock) where CategoryID=12 and used=1 and FitCode='Wish') "
               )
        tokens = self.run_sql(sql)
        return tokens

    def get_wish_orders(self, token):
        date = str(datetime.datetime.now() - datetime.timedelta(days=7))[:10]
        url = "https://china-merchant.wish.com/api/v2/order/multi-get"
        start = 0
        limit = 100
        try:
            while True:
                data = {
                    "access_token": token['AccessToken'],
                    "format": "json",
                    "start": start,
                    "limit": limit,
                    "since": date,
                    # "since": '2020-05-02',
                    # "upto": '2020-05-03'
                }
                res_dict = dict()

                for i in range(3):
                    try:
                        r = requests.get(url, params=data, timeout=10)
                        res_dict = json.loads(r.content)
                        break
                    except Exception as why:
                        self.logger.error(f'fail get refund of {token["aliasname"]} {i + 1} times cause of {why}')
                if res_dict:
                    orders = res_dict['data']
                    for order in orders:
                        order_detail = order["Order"]
                        if 'refunds' in order_detail:
                            all_refunds = order_detail['refunds']
                            for refunds_info in all_refunds:
                                refunds = refunds_info['RefundsInfo']
                                refunds['aliasname'] = token['aliasname']
                                refunds['order_id'] = order_detail['order_id']
                                refunds['_id'] = order_detail['transaction_id']
                                refunds['plat'] = 'wish'
                                self.put(refunds)
                    start += limit
                    if len(orders) < limit:
                        break
                else:
                    break

        except Exception as e:
            self.logger.error('{} fails cause of {}'.format(token['aliasname'], e))

    def clean(self):
        self.col.delete_many({})
        self.logger.info('success to clean data')

    def put(self, row):
        # self.logger.info(f'saving {row["order_id"]}')
        self.col.update_one({'_id': row['_id']}, {"$set": row}, upsert=True)

    def pull(self):
        ret = self.col.find()
        for row in ret:
            yield row

    def push(self, row):
        sql = ("if not EXISTS (select id from y_refunded(nolock) where "
               "order_id=%s and refund_time= %s) "
               "insert into y_refunded (order_id,refund_time, total_value,currencyCode,plat) "
               "values (%s,%s,%s,%s, %s) "
               "else update y_refunded set "
               "total_value=%s where order_id=%s and refund_time= %s")
        try:
            self.cur.execute(sql,
                             (row['order_id'], row['refund_time'],
                              row['order_id'], row['refund_time'],
                              row['merchant_responsible_amount'], row['currency_code'], row['plat'],
                              row['merchant_responsible_amount'], row['order_id'], row['refund_time']))
            self.con.commit()
            self.logger.info('save %s' % row['order_id'])
        except Exception as e:
            self.logger.error(f'fail to save {row["order_id"]} cause of duplicate key or {e}')

    def save_trans(self):
        rows = self.pull()
        for row in rows:
            self.push(row)

    def run(self):
        try:
            tokens = self.get_wish_token()
            self.clean()
            with ThreadPoolExecutor(16) as pl:
                pl.map(self.get_wish_orders, tokens)
            self.save_trans()

        except Exception as e:
            self.logger.error(e)
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            mongo.close()
            self.close()


if __name__ == "__main__":
    worker = WishRefund()
    worker.run()




