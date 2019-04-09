#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-04-08 17:11
# Author: turpure

from src.services.base_service import BaseService


class EbayHandler(BaseService):
    """
    get trades whose receiverBusiness is empty and full with the right payPal email
    """

    def get_trades(self):
        sql = ("select pt.nid,ptd.l_number,'p_trade' as tableName from p_trade(nolock) as pt "
               "LEFT JOIN p_tradedt(nolock) as ptd on pt.nid= ptd.tradeNid "
               "where addressowner='ebay' and isnull(receiveremail,'') = '' "
               "and convert(varchar(7),closingdate,121)='2019-03' union "
               "select pt.nid, ptd.l_number,'P_trade_his' as tableName from p_trade_his(nolock) as pt "
               "LEFT JOIN p_tradedt_his(nolock) as ptd on pt.nid= ptd.tradeNid "
               "where addressowner='ebay' and isnull(receiveremail,'') = '' "
               "and convert(varchar(7),closingdate,121)='2019-03'")
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_paypal(self, row):
        sql = ("select top 1  nid ,l_number, payPal from "
               "(select pt.nid,ptd.l_number,receiverBusiness as payPal "
               "from p_trade(nolock) as pt LEFT JOIN p_tradedt(nolock) as ptd "
               "on pt.nid= ptd.tradeNid where addressowner='ebay' and isnull(receiveremail,'') != '' "
               "and l_number=%s UNION "
               "select pt.nid,ptd.l_number,receiverBusiness as payPal "
               "from p_trade_his(nolock) as pt LEFT JOIN p_tradedt_his(nolock) as ptd "
               "on pt.nid= ptd.tradeNid where addressowner='ebay' and isnull(receiveremail,'') != '' "
               "and l_number=%s) tap")
        try:
            self.cur.execute(sql, (row['l_number'], row['l_number']))
            ret = self.cur.fetchone()
            row['payPal'] = ret['payPal']
            return row
        except Exception as why:
            self.logger.error('{} fails to get payPal cause of {}'.format(row['nid'], why))

    def set_paypal(self, row):
        sql = 'update {} set receiveremail=%s where nid=%s'
        try:
            self.cur.execute(sql.format(row['tableName']), (row['payPal'], row['nid']))
            self.con.commit()
            self.logger.info('set {} with payPal: {}'.format(row['nid'], row['payPal']))
        except Exception as why:
            self.logger.error('fail to set {} cause of {}'.format(row['nid'], why))

    def run(self):
        try:
            for row in self.get_trades():
                paypal_row = self.get_paypal(row)
                if paypal_row:
                    self.set_paypal(paypal_row)
        except Exception as why:
            self.logger.error(why)
        finally:
            self.close()


if __name__ == '__main__':
    worker = EbayHandler()
    worker.run()
