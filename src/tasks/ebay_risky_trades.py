#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-11-26 10:39
# Author: turpure

from src.services.base_service import BaseService
import datetime


class RiskController(BaseService):
    """
    get risky trades
    """
    def __init__(self):
        super().__init__()

    def get_blacklist(self):
        sql = 'select * from oauth_blacklist'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    @staticmethod
    def generate_query(blacklist):
        base_query = ("select dateadd(hour,8,ordertime) as orderTime,suffix,nid,"
                      "buyerId,shipToName,shipToStreet,shipToStreet2,shipToCity,"
                      "shipToZip,shipToCountryCode,shipToPhoneNum,"
                      "'p_tradeun' as tablename "
                      "from P_Tradeun with(nolock) where "
                      " memo not like '%钓鱼账号%' and  protectioneligibilitytype='缺货订单' and   "
                      "dateadd(hour,8,ordertime) between dateadd(day,-4,getdate()) and getdate() and "
                      "  {} union "
                      "select dateadd(hour,8,ordertime) as orderTime,suffix,nid,"
                      "buyerId,shipToName,shipToStreet,shipToStreet2,shipToCity,"
                      "shipToZip,shipToCountryCode,shipToPhoneNum,"
                      "'p_trade' as tablename "
                      "from P_Trade with(nolock) where "
                      " memo not like '%钓鱼账号%' and  "
                      " dateadd(hour,8,ordertime) between dateadd(day,-4,getdate()) and getdate() and "
                      " {}"
                      )
        filed_query = []
        for filed, value in blacklist.items():
            if value and filed != 'id':
                if '%' in value:
                    filed_query.append("{} like '{}'".format(filed, value))
                else:
                    filed_query.append("{} = '{}'".format(filed, value))
        filed_query = ' and '.join(filed_query)
        query = base_query.format(filed_query, filed_query)
        return query

    def risky_trades(self, query):
        self.cur.execute(query)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_trades_info(self):
        risky_trades = []
        for ele in self.get_blacklist():
            sql = self.generate_query(ele)
            trades = self.risky_trades(sql)
            for trade in trades:
                if trade:
                    if not trade['nid'] in risky_trades:
                        risky_trades.append(trade['nid'])
                        yield trade

    def save_to_base(self, trades):
        sql = ('insert into riskyTrades (tradeNid,orderTime,suffix,buyerId,'
               'shipToName,shipToStreet,shipToStreet2,shipToCity,shipToZip,'
               'shipToCountryCode,shipToPhoneNum,completeStatus) '
               'values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)')
        for row in trades:
            try:
                self.warehouse_cur.execute(sql, (row['nid'], row['orderTime'], row['suffix'],
                                           row['buyerId'], row['shipToName'], row['shipToStreet'],
                                           row['shipToStreet2'], row['shipToCity'], row['shipToZip'],
                                           row['shipToCountryCode'], row['shipToPhoneNum'], '待处理'))
                self.warehouse_con.commit()
                self.logger.info('putting risky trade {}'.format(row['nid']))
            except Exception as e:
                self.logger.error(e)

    def intercept(self, trades):
        cur = self.cur
        max_bill_code_query = "P_S_CodeRuleGet 140,''"
        exception_trade_handler = "p_exceptionTradeToException %s,4,'其它异常单',%s"
        normal_trade_handler = 'www_normal2exception %s,%s'
        free_reservation = """if(isnull((select top 1 filterflag from P_trade where nid=%s),0)>5)
                                BEGIN EXEC P_KC_FreeReservationNum %s end"""

        update_trade_dt_un = 'update p_tradedtun set L_shippingamt=1 where tradenid =%s'
        update_trade_un = ("update p_tradeun set memo = isnull(memo,'') + ' ' + %s+':疑似eBay钓鱼账号',"
                           "reasoncode = 'eBay钓鱼账号' where nid=%s")
        cur_time = str(datetime.datetime.now())
        try:
            cur.execute(max_bill_code_query)
            max_bill_code = cur.fetchone()['MaxBillCode']
            for row in trades:
                nid = row['nid']
                table_name = row['tablename']
                if table_name == 'p_trade':
                    cur.execute(free_reservation, (nid, nid))
                    cur.execute(normal_trade_handler, (nid, max_bill_code))
                if table_name == 'p_tradeun':
                    cur.execute(exception_trade_handler, (nid, max_bill_code))

                cur.execute(update_trade_un, (cur_time, nid))
                cur.execute(update_trade_dt_un, (nid,))
                self.logger.info('%s from %s may be bad trade' % (nid, table_name))
            self.con.commit()
        except Exception as e:
            self.logger.error(e)

    def work(self):
        trades = [trade for trade in self.get_trades_info()]
        try:
            self.intercept(trades)
            self.save_to_base(trades)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.close()


if __name__ == '__main__':
    worker = RiskController()
    worker.work()
