#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-19 16:26
# Author: turpure


import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.services.base_service import BaseService
from configs.config import Config
import requests


class VovaFee(BaseService):
    """
    fetch ebay fee using api
    """
    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')
        # self.begin_date = str(datetime.datetime.today() - datetime.timedelta(days=4))[:10]
        self.begin_date = '2020-05-01'


    def clean(self):
        sql = "delete from y_refunded WHERE refund_time >= %s AND plat='vova'"
        self.cur.execute(sql, self.begin_date)
        self.con.commit()


    def get_vova_token(self):
        sql = 'SELECT AliasName AS suffix,MerchantID AS selleruserid,APIKey AS token FROM [dbo].[S_SyncInfoVova] WHERE SyncInvertal=0;'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row




    def get_vova_fee(self, token):
        url = 'https://merchant-api.vova.com.hk/v1/order/ChangedOrders'
        limit = 200
        try:
            for i in range(0, 1000):
                param = {
                    "token": token['token'],
                    "since": self.begin_date,
                    "limit": limit,
                    'start': i * limit
                }
                response = requests.get(url, params=param)
                ret = response.json()
                if ret['code'] == 20000 and ret['data']['order_list']:
                    for row in ret['data']['order_list']:
                        if row['refund_time']:
                            refunds = dict()
                            refunds['order_id'] = row['order_goods_sn']
                            refunds['refund_time'] = row['refund_time']
                            refunds['total_value'] = row['total_amount']
                            refunds['currencyCode'] = row['currency']
                            refunds['plat'] = 'vova'
                            # print(row['refund_time'])
                            # yield (row['order_goods_sn'], row['refund_time'], row['total_amount'], row['currency'], 'vova')
                            yield refunds

                    if len(ret['data']['order_list']) < limit:
                        break
                else:
                    break

        except Exception as e:
            self.logger.error(e)


    def save_data(self, row):
        # sql = 'insert into y_refunded(order_id, refund_time, total_value, currencyCode, plat) values(%s,%s,%s,%s,%s)'
        sql = ("if not EXISTS (select id from y_refunded(nolock) where "
               "order_id=%s and refund_time=%s and plat=%s) "
               "insert into y_refunded (order_id, refund_time, total_value, currencyCode, plat) "
               "values (%s,%s,%s,%s,%s) "
               "else update y_refunded set "
               "total_value=%s where order_id=%s and refund_time= %s and plat=%s")
        try:
            self.cur.execute(sql, (row['order_id'], row['refund_time'], row['plat'],
                        row['order_id'], row['refund_time'],
                        row['total_value'], row['currencyCode'], row['plat'],
                        row['total_value'], row['order_id'], row['refund_time'], row['plat']))
            self.con.commit()
            self.logger.info('save %s' % row['order_id'])
        except Exception as e:
            self.logger.error(f'fail to save {row["order_id"]} cause of duplicate key or {e}')




    def run(self):
        try:
            # self.clean()
            tokens = self.get_vova_token()
            with ThreadPoolExecutor(16) as pool:
                future = {pool.submit(self.get_vova_fee, token): token for token in tokens}
                for fu in as_completed(future):
                    try:
                        data = fu.result()
                        for row in data:
                            # print(row)
                            self.save_data(row)
                    except Exception as e:
                        self.logger.error(e)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.close()


if __name__ == '__main__':
    worker = VovaFee()
    worker.run()




