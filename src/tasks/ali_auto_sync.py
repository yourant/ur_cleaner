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

    def close(self):
        self.base_dao.close_cur(self.cur)

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
            self.logger.error('error while get order details about %s cause of %s' % (order_id, e))
            return out

    def check_order(self, check_info):
        order_id = check_info['order_id']
        search_sql = ("select cgsm.NID,cgsm.billnumber," 
                    "sum(sd.amount) total_amt," 
                    "sum(sd.amount*sd.price) as total_money, "
                    "sum(sd.amount * gs.costprice) as total_cost_money "   # 2020-06-22  修改
                    "from cg_stockorderd(nolock)  as sd "
                    "LEFT JOIN cg_stockorderm(nolock)  as cgsm on sd.stockordernid= cgsm.nid " 
                    "LEFT JOIN b_goodssku(nolock)  as gs on sd.goodsskuid= gs.nid " 
                    "where alibabaOrderid = %s " 
                    "GROUP BY cgsm.billnumber, cgsm.nid,cgsm.recorder," 
                    "cgsm.expressfee,cgsm.audier,cgsm.audiedate,cgsm.checkflag ")

        check_sql = "P_CG_UpdateStockOutOfByStockOrder %s"
        update_sql = ("update cg_stockorderM  set alibabaorderid=%s," 
                     "expressFee=%s, alibabamoney=%s, ordermoney=%s" 
                     "where billNumber = %s")

        update_price = "update cgd set money= gs.costprice * amount + amount*(%s-%s)/%s," \
                       "allmoney= gs.costprice * amount + amount*(%s-%s)/%s, " \
                       "cgd.beforeavgprice= gs.costprice, " \
                       "cgd.price= gs.costprice  + (%s-%s)/%s ," \
                       "cgd.taxprice= gs.costprice + (%s-%s)/%s " \
                       "from cg_stockorderd  as cgd " \
                       "LEFT JOIN B_goodsSku as gs on cgd.goodsskuid = gs.nid " \
                       "LEFT JOIN cg_stockorderm as cgm on cgd.stockordernid= cgm.nid " \
                       "where billnumber=%s"
        log_sql = "INSERT INTO CG_StockLogs(OrderType,OrderNID,Operator,Logs) VALUES(%s,%s,%s,%s)"
        try:
            self.cur.execute(search_sql, order_id)
            ret = self.cur.fetchone()
            if ret:
                qty = ret['total_amt']
                stock_nid = ret['NID']
                total_cost_money = ret['total_cost_money']
                bill_number = ret['billnumber']
                check_qty = check_info['qty']
                order_money = check_info['sumPayment']
                express_fee = check_info['expressFee']
                if qty == check_qty:
                    log = 'ur_cleaner ' + str(datetime.datetime.today())[:19] + " 同步1688订单差额"
                    self.cur.execute(update_sql, (order_id, express_fee, order_money, order_money, bill_number))
                    self.cur.execute(update_price, (order_money, total_cost_money, qty) * 4 + (bill_number,))
                    self.cur.execute(check_sql, (bill_number,))
                    self.cur.execute(log_sql, ('采购订单', ret['NID'], 'ur_cleaner', log))
                    self.con.commit()
                    self.logger.info('checking %s' % bill_number)

                # 不论数量一致不，都把实付款-订单总额的差额分摊下去
                else:

                    # 记录日志
                    log = 'ur_cleaner ' + str(datetime.datetime.today())[:19] + " 同步1688订单差额"

                    # 差额= 总的成本金额-1688实付款
                    select_delta_sql = ('select alibabaMoney  from cg_stockOrderM(nolock) '
                                        'where nid = %s')
                    self.cur.execute(select_delta_sql, (stock_nid,))
                    delta_money = 0
                    row = self.cur.fetchone()
                    if row:
                        delta_money = total_cost_money - row['alibabaMoney']
                    single_delta_money = delta_money / qty


                    # 更新含税价格
                    set_tax__price_sql = ('update cgd set  cgd.beforeavgprice= gs.costprice,cgd.price = gs.costprice - %s, '
                                          'taxPrice = gs.costprice - %s,money = amount * gs.costprice, '
                                          'allMoney = amount * (gs.costprice - %s) from cg_stockOrderD as cgd '
                                          'Left JOIN B_goodsSku(nolock) as gs on cgd.goodsskuid = gs.nid '
                                          'where cgd.stockOrderNid = %s')
                    self.cur.execute(set_tax__price_sql, (single_delta_money,single_delta_money, single_delta_money, stock_nid,))

                    # 审核订单
                    self.cur.execute(check_sql, (bill_number,))
                    self.cur.execute(log_sql, ('采购订单', stock_nid, 'ur_cleaner', log))
                    self.con.commit()
                    self.logger.info('checking %s' % bill_number)

        except Exception as e:
            self.con.rollback()
            self.logger.error('%s while checking %s' % (e, order_id))

    def get_order_from_py(self):
        some_days = str(datetime.datetime.today() - datetime.timedelta(days=60))[:10]
        query = ("select DISTINCT billNumber,alibabaOrderid as orderId,case when loginId like 'caigoueasy%' then "
                " 'caigoueasy' else loginId end  as account ,MakeDate "
                "from CG_StockOrderD(nolock)  as cd   "
                "LEFT JOIN CG_StockOrderM(nolock)  as cm  on cd.stockordernid = cm.nid  "
                "LEFT JOIN S_AlibabaCGInfo(nolock) as info  on Cm.AliasName1688 = info.AliasName  "
                "LEFT JOIN B_GoodsSKU(nolock) as g  on cd.goodsskuid = g.nid  "
                "where  CheckFlag=1 And inflag=0 ANd Archive=0 " # 采购已审核未入库
                 "AND MakeDate > %s  AND isnull(loginId,'') LIKE 'caigoueasy%' " # 是1688订单
                 "AND StoreID IN (2,7,36) "  # 金皖399  义乌仓 七部仓库 # 仓库限制
                 "AND ABS(OrderMoney - alibabamoney) > 0.1 " # 有差额的才同步
                # "where BillNumber = 'CGD-2021-01-13-0284' "
                # "and alibabaOrderid = '1069212930532682293' "
                " order by MakeDate "
                )
        self.cur.execute(query, (some_days,))
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
        begin = time.time()
        try:
            orders = self.get_order_from_py()

            for order in orders:
                self.check(order)
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



