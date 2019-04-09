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
        sql = ("select top 10 pt.nid,ptd.l_number from p_trade(nolock) as pt "
               "LEFT JOIN p_tradedt(nolock) as ptd on pt.nid= ptd.tradeNid "
               "where addressowner='ebay' and isnull(receiverbusiness,'') = '' "
               "and convert(varchar(7),closingdate,121)='2019-03'")
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_payPal(self, row):
        sql = ("select top 1 pt.nid,ptd.l_number,receiverBusiness as payPal from p_trade(nolock) as pt "
               "LEFT JOIN p_tradedt(nolock) as ptd on pt.nid= ptd.tradeNid "
               "where addressowner='ebay' and isnull(receiverbusiness,'') != '' and l_number=%s")
        self.cur.execute(sql, row['l_number'])
        ret = self.cur.fetchone()
        row['payPal'] = ret['payPal']
        return row

    def run(self):
        try:
            for row in self.get_trades():
                print(self.get_payPal(row))
        except Exception as why:
            self.logger.error(why)
        finally:
            self.close()


if __name__ == '__main__':
    worker = EbayHandler()
    worker.run()
