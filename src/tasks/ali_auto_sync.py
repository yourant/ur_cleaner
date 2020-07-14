#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-20 15:56
# Author: turpure

import json
import requests
import datetime
from tenacity import retry, stop_after_attempt
from src.services.base_service import BaseService
from src.services import oauth as aliOauth


class AliSync(BaseService):
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
            ret = json.loads(res.content)['result']
            out['order_id'] = order_id
            out['expressFee'] = float(ret['baseInfo']['shippingFee'])
            out['sumPayment'] = float(ret['baseInfo']['totalAmount'])
            out['qty'] = sum([ele['quantity'] for ele in ret['productItems']])
            return out
        except Exception as e:
            self.logger.error('error while get order details %s' % e)
            return out

    def check_order(self, check_info):
        order_id = check_info['order_id']
        search_sql = ("select cgsm.billnumber," 
                    "sum(sd.amount) total_amt," 
                    "sum(sd.amount*sd.price) as total_money, "
                    "sum(sd.amount * gs.costprice) as total_cost_money "   # 2020-06-22  修改
                    "from cg_stockorderd  as sd "
                    "LEFT JOIN cg_stockorderm  as cgsm on sd.stockordernid= cgsm.nid " 
                    "LEFT JOIN b_goodssku  as gs on sd.goodsskuid= gs.nid " 
                    "where alibabaOrderid = %s " 
                    "GROUP BY cgsm.billnumber, cgsm.nid,cgsm.recorder," 
                    "cgsm.expressfee,cgsm.audier,cgsm.audiedate,cgsm.checkflag ")

        check_sql = "P_CG_UpdateStockOutOfByStockOrder %s"

        # update_status = "update cg_stockorderM  set ordermoney=%s where billNumber = %s"

        update_sql = ("update cg_stockorderM  set alibabaorderid=%s," 
                     # "expressFee=%s-%s, alibabamoney=%s " 
                     "expressFee=%s, alibabamoney=%s, ordermoney=%s" 
                     "where billNumber = %s")

        update_price = "update cgd set money= gs.costprice * amount + amount*(%s-%s)/%s," \
                       "allmoney= gs.costprice * amount + amount*(%s-%s)/%s, " \
                       "cgd.beforeavgprice= gs.costprice, " \
                       "cgd.price= gs.costprice ," \
                       "cgd.taxprice= gs.costprice + (%s-%s)/%s " \
                       "from cg_stockorderd  as cgd " \
                       "LEFT JOIN B_goodsSku as gs on cgd.goodsskuid = gs.nid " \
                       "LEFT JOIN cg_stockorderm as cgm on cgd.stockordernid= cgm.nid " \
                       "where billnumber=%s"


        try:
            self.cur.execute(search_sql,order_id)
            ret = self.cur.fetchone()
            if ret:
                qty = ret['total_amt']
                total_money = ret['total_money']
                total_cost_money = ret['total_cost_money']
                bill_number = ret['billnumber']
                check_qty = check_info['qty']
                order_money = check_info['sumPayment']
                expressFee = check_info['expressFee']
                # if qty == check_qty:
                self.cur.execute(update_sql, (order_id, expressFee, order_money, order_money, bill_number))
                self.cur.execute(check_sql, (bill_number,))
                self.cur.execute(update_price, (order_money, total_money, qty) * 2 + (order_money, total_cost_money, qty) * 1 + (bill_number,))
                # self.cur.execute(update_status, (order_money, bill_number))
                self.con.commit()
                self.logger.info('checking %s' % bill_number)
        except Exception as e:
            self.logger.error('%s while checking %s' % (e, order_id))

    def get_order_from_py(self):
        today = str(datetime.datetime.today())[:10]

        someDays = str(datetime.datetime.today() - datetime.timedelta(days=7))[:10]
        # threeDays = str(datetime.datetime.strptime(today[:8] + '01', '%Y-%m-%d'))[:10]
        query = ("select DISTINCT billNumber,alibabaOrderid as orderId,case when loginId like 'caigoueasy%' then "
                " 'caigoueasy' else loginId end  as account "
                "from CG_StockOrderD  as cd with(nolock)  "
                "LEFT JOIN CG_StockOrderM  as cm with(nolock) on cd.stockordernid = cm.nid  "
                "LEFT JOIN S_AlibabaCGInfo as info with(nolock) on Cm.AliasName1688 = info.AliasName  "
                "LEFT JOIN B_GoodsSKU as g with(nolock) on cd.goodsskuid = g.nid  "
                "where  ABS(taxPrice-costPrice) > 0.1 AND MakeDate > %s  AND CheckFlag=1 AND isnull(loginId,'')<>'' "
                 "and cm.deptId != 46 "
                 # "where 1=1 "
                # "and alibabaOrderid = '1069212930532682293' "
                )


        self.cur.execute(query, (someDays))
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def check(self, order):
        try:
            ret = self.get_order_details(order)
            if ret:
                self.check_order(ret)
        except Exception as e:
            self.logger.error(e)

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
    worker = AliSync()
    worker.work()



