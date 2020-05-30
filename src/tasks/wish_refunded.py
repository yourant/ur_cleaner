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


class WishRefund(BaseService):
    """
    get refunded orders of wish
    """
    def __init__(self):
        super().__init__()

    def run_sql(self, sql):
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_wish_token(self):
        sql = ("SELECT AccessToken,aliasname FROM S_WishSyncInfo WHERE  " 
              # "  datediff(DAY,LastSyncTime,getdate())<5 and "
               "aliasname is not null"
                " and  AliasName not in "
               "(select DictionaryName from B_Dictionary where CategoryID=12 and used=1 and FitCode='Wish') "
               )
        tokens = self.run_sql(sql)
        return tokens

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
               "insert into y_refunded (order_id,refund_time, total_value,currencyCode,plat) "
               "values (%s,%s,%s,%s, 'wish') "
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
        try:
            tokens = self.get_wish_token()
            with ThreadPoolExecutor() as pool:
                ret = {pool.submit(self.get_wish_orders, token): token for token in tokens}
                for future in concurrent.futures.as_completed(ret):
                    try:
                        orders = future.result()
                        for order in orders:
                            self.save_data(order)
                    except Exception as e:
                        self.logger.error(e)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.close()


if __name__ == "__main__":
    worker = WishRefund()
    worker.run()




