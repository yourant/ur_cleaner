#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-20 15:56
# Author: turpure

import json
import requests
from tenacity import retry, stop_after_attempt
from src.services.base_service import BaseService
from src.services import oauth as aliOauth
import re
import datetime


class AliChecker(BaseService):
    """
    check purchased orders
    """
    def __init__(self):
        super().__init__()
        # self.oauth = oauth.Ali('tb853697605')

    @retry(stop=stop_after_attempt(3))
    def get_order_details(self, order_info):
        order_id = order_info['orderId']
        oauth = aliOauth.Ali(order_info['account'])
        base_url = oauth.get_request_url(order_id)
        out = dict()
        try:
            res = requests.get(base_url)
            response = json.loads(res.content)
            try:
                if response['success'] == 'true':
                    ret = response['result']
                    out['order_id'] = order_id
                    out['expressFee'] = float(ret['baseInfo']['shippingFee'])
                    out['sumPayment'] = float(ret['baseInfo']['totalAmount'])
                    out['qty'] = sum([ele['quantity'] for ele in ret['productItems']])
                    return out
                else:
                    self.logger.error('error while get order details %s' % response['errorMessage'])
                    return out
            except BaseException:
                self.logger.error('error while get order details %s' % response['error_message'])
                return out
        except Exception as e:
            self.logger.error('error while get order details %s' % e)
            return out

    def check_order(self, check_info):
        order_id = check_info['order_id']
        search_sql = "select cgsm.NID,cgsm.billnumber,cgsm.recorder,cgsm.audier,cgsm.checkflag," \
                    "cgsm.audiedate, sum(sd.amount) total_amt," \
                    "sum(sd.amount * gs.costprice) as total_cost_money, " \
                    "sum(sd.amount*sd.price) as total_money, cgsm.expressfee " \
                    "from cg_stockorderd   as sd with(nolock) LEFT JOIN cg_stockorderm  as cgsm with(nolock) " \
                    " on sd.stockordernid= cgsm.nid " \
                    "LEFT JOIN b_goodssku  as gs on sd.goodsskuid= gs.nid "\
                    "where note like '%" + order_id + "%'" \
                     "and cgsm.checkflag =0 GROUP BY cgsm.billnumber, cgsm.nid,cgsm.recorder," \
                     "cgsm.expressfee,cgsm.audier,cgsm.audiedate,cgsm.checkflag"

        check_sql = "P_CG_UpdateStockOutOfByStockOrder %s"

        update_status = "update cg_stockorderM  set checkflag =1, audier=%s,ordermoney=%s,audiedate=getdate() where billNumber = %s"

        update_sql = "update cg_stockorderM set isSubmit=1,is1688Order=1 , alibabaorderid=%s," \
                     "expressFee=%s, alibabamoney=%s where billNumber = %s"

        update_price = "update cgd set money= gs.costprice * amount + amount*(%s-%s)/%s," \
                       "allmoney= gs.costprice * amount + amount*(%s-%s)/%s, " \
                       "cgd.beforeavgprice= gs.costprice, " \
                       "cgd.price= gs.costprice ," \
                       "cgd.taxprice= gs.costprice + (%s-%s)/%s " \
                       "from cg_stockorderd  as cgd " \
                       "LEFT JOIN B_goodsSku as gs on cgd.goodsskuid = gs.nid " \
                       "LEFT JOIN cg_stockorderm as cgm on cgd.stockordernid= cgm.nid " \
                       "where billnumber=%s"

        log_sql = u"INSERT INTO CG_StockLogs(OrderType,OrderNID,Operator,Logs) VALUES(%s,%s,%s,%s)".encode("utf8")

        try:
            self.cur.execute(search_sql)
            ret = self.cur.fetchone()
            if not ret:
                self.logger.info('no need to check %s' % order_id)
                return
            qty = ret['total_amt']
            total_cost_money = ret['total_cost_money']
            bill_number = ret['billnumber']
            audier = ret['audier']
            check_qty = check_info['qty']
            express_fee = check_info['expressFee']
            order_money = check_info['sumPayment']
            if qty == check_qty:
                self.cur.execute(update_sql, (order_id, express_fee, order_money, bill_number))
                self.cur.execute(update_price, (order_money, total_cost_money, qty) * 3 + (bill_number,))
                self.cur.execute(update_status, (audier, order_money, bill_number))
                self.cur.execute(check_sql, (bill_number,))
                log = 'ur_cleaner ' + str(datetime.datetime.today())[:19] + " 同步1688订单差额"
                self.cur.execute(log_sql, ('采购订单', ret['NID'], 'ur_cleaner', log))
                self.con.commit()
                self.logger.info('checking %s' % bill_number)
            else:
                self.logger.warning('quantity is not same of %s' % order_id)
        except Exception as e:
            self.logger.error('%s while checking %s' % (e, order_id))

    def get_order_from_py(self):
        query = """select note,
             'caigoueasy'
            as account
            from cg_stockorderm  as cm with(nolock)
            LEFT JOIN S_AlibabaCGInfo as info with(nolock)
            on Cm.AliasName1688 = info.AliasName
            where checkflag=0 and datediff(day,makedate,getdate())<4
             and isnull(note,'') != '' -- and billNumber='CGD-2020-08-03-0801'
            Union
            select note,
             'caigoueasy'
            as account
            from cg_stockorderm  as cm with(nolock)
            LEFT JOIN S_AlibabaCGInfo as info with(nolock) on Cm.AliasName1688 = info.AliasName
            where alibabaorderid ='' and DATEDIFF(dd, MakeDate, GETDATE()) BETWEEN 0 and 4
            and CheckFlag=1 and Archive=0 and InFlag=0 and isnull(note,'') != '' 
            """
        self.cur.execute(query)
        ret = self.cur.fetchall()
        for row in ret:
                note = row['note']
                order_ids = re.findall(r': (\d+)', note)
                for order in order_ids:
                    if len(order) > 10:
                        item = {'orderId': order, 'account': row['account']}
                        yield item

    def check(self, order):
            ret = self.get_order_details(order)
            if ret:
                self.check_order(ret)

    def work(self):
        try:
            orders = self.get_order_from_py()
            for order in orders:
                self.check(order)
        except Exception as e:
            self.logger(e)
        finally:
            self.close()


if __name__ == "__main__":
    worker = AliChecker()
    worker.work()
