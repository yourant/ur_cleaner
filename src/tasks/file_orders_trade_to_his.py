#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-20 10:02
# Author: turpure

import json
import datetime
import requests
import concurrent
from concurrent.futures import ThreadPoolExecutor
from tenacity import retry, stop_after_attempt
from src.services.base_service import BaseService


class FileOrdersToHis(BaseService):
    """
    get refunded orders of wish
    """
    def __init__(self):
        super().__init__()

    def get_batch_number(self):
        sql = "exec  P_S_CodeRuleGet 230,'';"
        self.cur.execute(sql)
        ret = self.cur.fetchone()
        return ret

    def get_order_ids(self, begin, end):
        sql = "SELECT nid FROM P_Trade (nolock) WHERE FilterFlag = 100 AND CONVERT(VARCHAR(10),CLOSINGDATE,121) BETWEEN  %s AND %s"
        self.cur.execute(sql,(begin, end))
        ret = self.cur.fetchall()
        for row in ret:
            yield row


    @retry(stop=stop_after_attempt(3))
    def get_wish_orders(self, token):
        date = str(datetime.datetime.now() - datetime.timedelta(days=4))[:10]
        url = "https://china-merchant.wish.com/api/v2/order/multi-get"
        start = 0
        try:
            while True:
                data = {
                    "access_token": token['AccessToken'],
                    "format": "json",
                    "start": start,
                    "limit": 100,
                    "since": date,
                    # "upto": '2018-10-01'
                }
                try:
                    r = requests.get(url, params=data, timeout=10)
                    res_dict = json.loads(r.content)
                    orders = res_dict['data']
                    for order in orders:
                        try:
                            order_detail = order["Order"]
                            if 'refunds' in order_detail:
                                all_refunds = order_detail['refunds']
                                for refunds_info in all_refunds:
                                    refunds = refunds_info['RefundsInfo']
                                    refunds['aliasname'] = token['aliasname']
                                    refunds['order_id'] = order_detail['order_id']
                                    yield refunds
                        except Exception as e:
                            self.logger.debug(e)
                    order_number = len(orders)
                    if order_number >= 100:
                        start += 100
                    else:
                        break
                except Exception as e:
                    self.logger.debug(e)
                    break
                else:
                    break

        except Exception as e:
            self.logger.error('{} fails cause of {}'.format(token['aliasname'], e))

    def save_data(self, row):
        sql = ("if not EXISTS (select id from y_refunded(nolock) where "
               "order_id=%s and refund_time= %s) "
               "insert into y_refunded (order_id,refund_time, total_value,currencyCode) "
               "values (%s,%s,%s,%s) "
               "else update y_refunded set "
               "total_value=%s where order_id=%s and refund_time= %s")
        try:
            self.cur.execute(sql,
                             (row['order_id'], row['refund_time'],
                              row['order_id'], row['refund_time'],
                              row['merchant_responsible_amount'], row['currency_code'],
                              row['merchant_responsible_amount'], row['order_id'], row['refund_time']))
            self.con.commit()
            self.logger.info('save %s' % row['order_id'])
        except Exception as e:
            self.logger.error(f'fail to save {row["order_id"]} cause of duplicate key or {e}')


    def run(self):
        now = datetime.datetime.now()
        begin = '2019-01-01'
        end = datetime.datetime(now.year, now.month - 2, 1) - datetime.timedelta(1)
        # end = '2020-02-29'
        try:
            ids = self.get_order_ids(begin, end)
            batch_number = self.get_batch_number()
            item = []
            id_dict = []
            step = 50
            i = 0
            # len = sum(1 for _ in ids)
            for id in ids:
                item.append(str(id['nid']))
                if i != 0 and i%step == 0 :
                    id_dict.append(str(','.join(item)))
                    item = []
                i = i + 1
            for id in id_dict:
                sql = "exec P_ForwardTradeToHis %s,%s,'ur-cleaner'"
                self.cur.execute(sql, (id, batch_number))
                self.con.commit()


        except Exception as e:
            self.logger.error(e)
        finally:
            self.close()


if __name__ == "__main__":
    import time
    start = time.time()
    worker = FileOrdersToHis()
    worker.run()
    end = time.time()
    date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
    print(date + f' it takes {end - start} seconds')




