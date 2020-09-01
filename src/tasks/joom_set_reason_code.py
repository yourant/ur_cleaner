#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-06-20 20:02
# Author: turpure


import os
from src.services.base_service import CommonService


class Worker(CommonService):

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    def work(self):
        out_of_stock_sql = "update pt  set pt.reasoncode='没有问题，可以发' FROM P_tradeun(nolock) as pt  LEFT JOIN T_express (nolock) e ON e.nid = pt.expressnid  where addressowner='joom' and e.name in ('Joom-线上','云途物流')   and FilterFlag = 1 and profitmoney <= -10"
        four_px_sql = "update pt  set pt.reasoncode='没有问题，可以发'  FROM P_trade(nolock) as pt  LEFT JOIN B_LogisticWay (nolock) l ON l.nid = pt.logicsWayNID   LEFT JOIN T_express (nolock) e ON e.nid = pt.expressnid  WHERE addressowner='joom' and e.name in ('Joom-线上','云途物流') and  pt.FilterFlag = 6 AND l.EUB = 3 and profitmoney <= -10"
        try:
            self.cur.execute(out_of_stock_sql)
            self.cur.execute(four_px_sql)
            self.con.commit()
            self.logger.info('success to update reason code of joom')
        except Exception as why:
            self.logger.error('failed to update joom express-fare cause of {}'.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()
