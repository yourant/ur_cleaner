#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-30 15:07
# Author: turpure

import datetime
from src.services.base_service import BaseService


class Fetcher(BaseService):
    """
    fetch developer sold detail from erp and put them into data warehouse
    """

    def __init__(self):
        super().__init__()

    def fetch(self, date_flag, begin_date, end_date):
        sql = 'oauth_oauth_devGoodsSoldDetail @dateFlag=%s, @beginDate=%s, @endDate=%s'
        self.cur.execute(sql, (date_flag, begin_date, end_date))
        ret = self.cur.fetchall()
        for row in ret:
            yield (row['developer'],
                   row['goodsCode'],
                   row['devDate'],
                   row['goodsStatus'],
                   row['tradeNid'],
                   row['plat'],
                   row['suffix'],
                   int(row['sold']) if row['sold'] else 0,
                   float(row['amt']) if row['amt'] else 0,
                   float(row['profit']) if row['profit'] else 0,
                   row['dateFlag'],
                   row['orderTime'],
                   )

    def push(self, rows):
        sql = ('insert into cache_devGoodsSoldDetail('
               'developer,goodsCode,developDate,goodsStatus,tradeNid,plat,suffix,sold,amt,profit,dateFlag,orderTime) '
               'values(%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s)'
                ' ON DUPLICATE KEY UPDATE sold=values(sold),amt=values(amt),profit=values(profit)'
               )
        self.warehouse_cur.executemany(sql, list(rows))
        self.warehouse_con.commit()

    def clean(self):
        pass

    def work(self):
        try:
            today = str(datetime.datetime.today())
            four_days_ago = str(datetime.datetime.today() - datetime.timedelta(days=4))[:10]
            for date_flag in [0, 1]:
                rows = self.fetch(date_flag, four_days_ago, today)
                self.push(rows)
                self.logger.info('success to fetch dev sold details')
        except Exception as why:
            self.logger.error('fail to fetch dev sold details of {}'.format(why))
        finally:
            self.close()


if __name__ == '__main__':
    worker = Fetcher()
    worker.work()
