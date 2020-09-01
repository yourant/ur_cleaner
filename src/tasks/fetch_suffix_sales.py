#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-30 15:07
# Author: turpure

import datetime
from src.services.base_service import CommonService


class Fetcher(CommonService):
    """
    fetch suffix sales from erp and put them into data warehouse
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.warehouse = 'mysql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.warehouse_cur = self.base_dao.get_cur(self.warehouse)
        self.warehouse_con = self.base_dao.get_connection(self.warehouse)

    def close(self):
        self.base_dao.close_cur(self.cur)
        self.base_dao.close_cur(self.warehouse_cur)

    def fetch(self, date_flag, begin_date, end_date):
        sql = 'oauth_saleTrendy @dateFlag=%s, @beginDate=%s, @endDate=%s'
        self.cur.execute(sql, (date_flag, begin_date, end_date))
        ret = self.cur.fetchall()
        for row in ret:
            yield (row['suffix'],
                   row['orderTime'],
                   float(row['amt']) if row['amt'] else 0,
                   row['dateFlag'],
                   )

    def push(self, rows):
        sql = ('insert into cache_suffixSales(suffix,orderTime,amt,dateFlag) values(%s,%s,%s, %s)'
                ' ON DUPLICATE KEY UPDATE amt=values(amt)'
               )
        self.warehouse_cur.executemany(sql, list(rows))
        self.warehouse_con.commit()

    def clean(self, begin_date, end_date):
        sql = 'delete from cache_suffixSales where orderTime between %s and %s'
        self.warehouse_cur.execute(sql, (begin_date, end_date))
        self.warehouse_con.commit()
        self.logger.info(f'success to clear sales data between {begin_date} and {end_date}')

    def work(self):
        try:
            today = str(datetime.datetime.today())
            some_days_ago = str(datetime.datetime.today() - datetime.timedelta(days=30))[:10]
            self.clean(some_days_ago, str(today)[:10])
            for date_flag in [0, 1]:
                rows = self.fetch(date_flag, some_days_ago, str(today)[:10])
                self.push(rows)
                self.logger.info('success to fetch suffix sales')
        except Exception as why:
            self.logger.error('fail to fetch suffix sales cause of {}'.format(why))
        finally:
            self.close()


if __name__ == '__main__':
    worker = Fetcher()
    worker.work()
