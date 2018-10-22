#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-20 15:56
# Author: turpure

import json
from functools import reduce
from concurrent.futures import ThreadPoolExecutor
import requests
from tenacity import retry, stop_after_attempt
from src.services import db, log, oauth


class AliChecker(object):
    """
    check purchased orders
    """
    def __init__(self):
        self.con = db.Mssql().connection
        self.logger = log.SysLogger().log
        self.cur = self.con.cursor(as_dict=True)
        self.oauth = oauth.Ali()

    @retry(stop=stop_after_attempt(3))
    def get_order_details(self, order_id):
        base_url = self.oauth.get_request_url(order_id)
        out = dict()
        try:
            res = requests.get(base_url)
            ret = json.loads(res.content)['result']['toReturn'][0]
            out['order_id'] = order_id
            out['expressFee'] = (ret['carriage'] + 0.0) / 100
            out['sumPayment'] = (ret['sumPayment'] + 0.0) / 100
            out['qty'] = reduce(lambda x, y: x + y, [ele['quantity'] for ele in ret['orderEntries']])
            return out
        except Exception as e:
            self.logger.error('error while get order details %s' % e)

    def check_order(self, check_info):
        order_id = check_info['order_id']
        search_sql = "SELECT cgsm.billnumber,cgsm.recorder,cgsm.audier,cgsm.checkflag," \
                    "cgsm.audiedate, sum(sd.amount) total_amt," \
                    "sum(sd.amount*sd.price) AS total_money, cgsm.expressfee " \
                    "from cg_stockOrderd  AS sd LEFT JOIN cg_stockorderm  AS cgsm" \
                    " ON sd.stockordernid= cgsm.nid WHERE note LIKE '%" + order_id + "%'" \
                    "AND cgsm.checkflag =0 GROUP BY cgsm.billnumber, cgsm.nid,cgsm.recorder," \
                    "cgsm.expressfee,cgsm.audier,cgsm.audiedate,cgsm.checkflag"

        check_sql = "P_CG_UpdateStockOutOfByStockOrder %s"

        update_status = "UPDATE cg_stockorderM  SET checkflag =1, audier=%s, " \
                        "ordermoney=%s,audiedate=getdate() WHERE billNumber = %s"

        update_sql = "UPDATE cg_stockorderM  SET alibabaorderid=%s," \
                     "expressFee=%s-%s, alibabamoney=%s " \
                     "WHERE billNumber = %s"

        update_price = "UPDATE cgd SET money= money + amount*(%s-%s)/%s," \
                       "allmoney= money + amount*(%s-%s)/%s, " \
                       "cgd.beforeavgprice= cgd.price, " \
                       "cgd.price= cgd.price + (%s-%s)/%s," \
                       "cgd.taxprice= cgd.taxprice + (%s-%s)/%s " \
                       "FROM cg_stockorderd  AS cgd LEFT JOIN cg_stockorderm" \
                       " AS cgm ON cgd.stockordernid= cgm.nid " \
                       "WHERE billnumber=%s"
        try:
            self.cur.execute(search_sql)
            ret = self.cur.fetchone()
            qty = ret['total_amt']
            total_money = ret['total_money']
            bill_number = ret['billnumber']
            checker = ret['audier']
            check_qty = check_info['qty']
            order_money = check_info['sumPayment']
            if qty == check_qty:
                self.cur.execute(update_sql, (order_id, order_money, total_money, order_money, bill_number))
                self.cur.execute(check_sql, (bill_number,))
                self.cur.execute(update_price, (order_money, total_money, qty) * 4 + (bill_number,))
                self.cur.execute(update_status, (checker, order_money, bill_number))
                self.con.commit()
                self.logger.info('checking %s' % bill_number)
        except Exception as e:
            self.logger.error('%s while checking %s' % (e, order_id))
            raise Exception(e)

    def get_order_from_py(self):
        query = ("select alibabaorderid as orderId from CG_StockOrderM "
                "where CheckFlag=0 and DATEDIFF(day, MakeDate, GETDATE())<30 "
                "and isnull(alibabaorderid,'')!=''")
        self.cur.execute(query)
        ret = self.cur.fetchall()
        for row in ret:
            yield row['orderId']

    def run(self):
        try:
            orders = self.get_order_from_py()
            pool = ThreadPoolExecutor()
            ret = pool.map(self.get_order_details, orders)
            for item in ret:
                self.check_order(item)
        except Exception as e:
            self.logger.error(e)


if __name__ == "__main__":
    worker = AliChecker()
    worker.run()
