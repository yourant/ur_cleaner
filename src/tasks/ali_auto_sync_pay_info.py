#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-20 15:56
# Author: turpure


import os
import json
import requests
import datetime
import time
from src.services.base_service import CommonService
from src.services import oauth as aliOauth


class AliSync(CommonService):
    """
    check purchased orders
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.payments = self.get_py_payment_method()
        self.begin =  str(datetime.datetime.today() - datetime.timedelta(days=10))[:10]
        self.end =  str(datetime.datetime.today())[:10]

    def close(self):
        self.base_dao.close_cur(self.cur)

    def get_py_payment_method(self):
        sql = ('select DictionaryName as name,alibabatradeType as tradeType,nid as balanceId '
               'from B_Dictionary where CategoryID=1 and len(alibabatradeType)>1 ')
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        payment = dict()
        for row in ret:
            payment[row['tradeType']] = row['balanceId']
        return payment

    def get_order_details(self, order_info):
        order_id = order_info['orderId']
        oauth = aliOauth.Ali(order_info['account'])
        out = dict()
        try:
            base_url = oauth.get_request_url(order_id)
            res = requests.get(base_url)
            ret = json.loads(res.content)['result']
            out['nid'] = order_info['nid']
            out['tradeTypeCode'] = ret['baseInfo']['tradeTypeCode']
            out['balanceId'] = self.payments[out['tradeTypeCode']]
            out['billNumber'] = order_info['billNumber']
            return out
        except Exception as e:
            self.logger.error(f'fait to get info of order {order_info["orderId"]} cause of {e}')
        return None

    def get_order_from_py(self):
        some_days = str(datetime.datetime.today() - datetime.timedelta(days=60))[:10]
        query = ("select nid, alibabaOrderId as orderId,billNumber, 'caigoueasy' as account from cg_stockOrderM(nolock) "
                 "where convert(varchar(10),makeDate,121) between %s and %s and CheckFlag=1 "
                 "and  not EXISTS ( select OrderNID from  CG_StockLogs(nolock) where cg_stockOrderM.nid =CG_StockLogs.OrderNid  and logs like '%同步1688订单付款方式%')")
                 # "where alibabaOrderId='1288587110981682293' ")
        self.cur.execute(query, (self.begin, self.end))
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def update_order_balanced_id(self, order):
        try:
            sql = 'update cg_stockOrderM set balanceId= %s where nid = %s'
            log_sql = u"INSERT INTO CG_StockLogs(OrderType,OrderNID,Operator,Logs) VALUES('采购订单',%s,'ur_cleaner',%s)".encode("utf8")
            log = 'ur_cleaner ' + str(datetime.datetime.today())[:19] + " 同步1688订单付款方式"
            self.cur.execute(sql, (order['balanceId'], order['nid']))
            self.cur.execute(log_sql, (order['nid'], log))
            self.con.commit()
            self.logger.info(f'success to update {order["billNumber"]} to {order["tradeTypeCode"]}')
        except Exception as why:
            print(self.logger.error(f'fail to update {order["billNumber"]} cause of {why}'))

    def work(self):
        begin = time.time()
        try:
            orders = self.get_order_from_py()
            for od in orders:
                order_data = self.get_order_details(od)
                if order_data:
                    self.update_order_balanced_id(order_data)
        except Exception as e:
            self.logger.error(f'fail to finish work of ali syncing cause of {e}')
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()
            self.logger.info(f'it takes {time.time() - begin}')


if __name__ == "__main__":
    worker = AliSync()
    worker.work()



