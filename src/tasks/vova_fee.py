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
        self.begin_date = str(datetime.datetime.today() - datetime.timedelta(days=1))[:10]
        # self.begin_date = '2020-05-01'


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
        param = {
            "token": token['token'],
            # "since": '2020-05-01T00:00:00.000Z',
            "since": self.begin_date,
            "limit": 200
        }

        url = 'https://merchant-api.vova.com.hk/v1/order/ChangedOrders'
        try:
            response = requests.get(url, params=param)
            ret = response.json()
            print(len(ret['data']['order_list']))
            print(ret)
            if ret['code'] == 20000 and ret['data']['order_list']:
                for row in ret['data']['order_list']:
                    if row['refund_time']:
                        # print(row['refund_time'])
                        yield (row['order_goods_sn'], row['refund_time'], row['total_amount'], row['currency'], 'vova')

        except Exception as e:
            self.logger.error(e)


    def save_data(self, row):
        sql = 'insert into y_refunded(order_id, refund_time, total_value, currencyCode, plat) values(%s,%s,%s,%s,%s)'
        try:
            self.cur.executemany(sql, row)
            self.con.commit()
            self.logger.error("success to get vova refunded order!")
        except Exception as e:
            self.logger.error("failed to get vova refunded order cause of %s" % e)




    def run(self):
        try:
            self.clean()
            tokens = self.get_vova_token()
            with ThreadPoolExecutor(16) as pool:
                future = {pool.submit(self.get_vova_fee, token): token for token in tokens}
                for fu in as_completed(future):
                    try:
                        data = fu.result()
                        # for row in data:
                        #     print(row)
                        self.save_data(data)
                    except Exception as e:
                        self.logger.error(e)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.close()


if __name__ == '__main__':
    worker = VovaFee()
    worker.run()




