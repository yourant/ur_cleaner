#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-19 13:13
# Author: turpure

import datetime
from src.services.base_service import CommonService


class EbayInterceptor(CommonService):
    """
    intercept ebay fraud trades
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    def get_trades_info(self):
        sql = 'www_ebay_bad_trades'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def intercept(self):
        cur = self.cur
        max_bill_code_query = "P_S_CodeRuleGet 140,''"
        exception_trade_handler = "p_exceptionTradeToException %s,4,'其它异常单',%s"
        normal_trade_handler = 'www_normal2exception %s,%s'
        free_reservation = """if(isnull((select top 1 filterflag from P_trade(nolock) where nid=%s),0)>5)
                                BEGIN EXEC P_KC_FreeReservationNum %s end"""

        update_trade_dt_un = 'update p_tradedtun set L_shippingamt=1 where tradenid =%s'
        update_trade_un = "update p_tradeun set memo = isnull(memo,'') + ' ' + %s+':疑似eBay钓鱼账号',reasoncode = 'eBay钓鱼账号' where nid=%s"
        cur_time = str(datetime.datetime.now())
        try:
            cur.execute(max_bill_code_query)
            max_bill_code = cur.fetchone()['MaxBillCode']
            trades = self.get_trades_info()

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
            self.logger.debug(e)

        finally:
            self.close()


if __name__ == '__main__':
    worker = EbayInterceptor()
    worker.intercept()



