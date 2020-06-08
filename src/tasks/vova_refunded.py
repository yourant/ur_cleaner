#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-19 16:26
# Author: turpure


import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.services.base_service import BaseService
import requests


class VoVaWorker(BaseService):
    """
    get VoVa Refund
    """
    def __init__(self):
        super().__init__()
        self.begin_date = str(datetime.datetime.today() - datetime.timedelta(days=4))[:10]

    def get_vova_token(self):
        sql = "SELECT AliasName AS suffix,MerchantID AS selleruserid,APIKey AS token FROM [dbo].[S_SyncInfoVova] WHERE SyncInvertal=0;"
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_vova_fee(self, token):
        url = 'https://merchant-api.vova.com.hk/v1/order/ChangedOrders'
        limit = 200
        start = 0
        try:
            while True:
                param = {
                    "token": token['token'],
                    "since": self.begin_date,
                    "limit": limit,
                    'start': start
                }
                response = requests.get(url, params=param)
                ret = response.json()
                # print(ret)
                if ret['code'] == 20000 and ret['data']['order_list']:
                    for row in ret['data']['order_list']:
                        if row['refund_time'] and int(row['order_state']) == 2:
                            refunds = dict()
                            refunds['order_id'] = row['order_goods_sn']
                            refunds['refund_time'] = row['refund_time']
                            refunds['total_value'] = row['total_amount']
                            refunds['currencyCode'] = row['currency']
                            refunds['platform_rate'] = row['platform_rate']
                            refunds['plat'] = 'vova'
                            yield refunds
                    start += limit
                    if len(ret['data']['order_list']) < limit:
                        break
                else:
                    break

        except Exception as e:
            self.logger.error(e)

    def save_data(self, row):
        # 确认普源订单状态
        check_sql = ("SELECT * FROM P_TradeUn m WHERE PROTECTIONELIGIBILITYTYPE='取消订单' AND (ACK=%s "
                    " OR EXISTS ( SELECT TOP 1 Z.MergeBillID FROM P_trade_b (nolock) Z WHERE m.NID = MergeBillID AND z.ACK=%s ))"
                    " UNION SELECT * FROM P_TradeUn_His m WHERE PROTECTIONELIGIBILITYTYPE='取消订单' AND (ACK=%s "
                    " OR EXISTS (SELECT TOP 1 Z.MergeBillID FROM P_trade_b (nolock) Z WHERE m.NID = MergeBillID AND z.ACK=%s))")
        self.cur.execute(check_sql, (row['order_id'], row['order_id'], row['order_id'], row['order_id']))
        ret = self.cur.fetchall()
        if ret:
            row['total_value'] = round(float(row['total_value']) * float(row['platform_rate']), 2)

        sql = ("if not EXISTS (select id from y_refunded(nolock) where "
               "order_id=%s and plat=%s) "
               "insert into y_refunded (order_id, refund_time, total_value, currencyCode, plat) "
               "values (%s,%s,%s,%s,%s) "
               "else update y_refunded set "
               "total_value=%s,refund_time=%s where order_id=%s and plat=%s")
        try:
            self.cur.execute(sql, (row['order_id'], row['plat'],
                        row['order_id'], row['refund_time'],
                        row['total_value'], row['currencyCode'], row['plat'],
                        row['total_value'], row['refund_time'], row['order_id'], row['plat']))
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




