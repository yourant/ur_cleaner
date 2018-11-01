#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-30 15:07
# Author: turpure

from src.services.base_service import BaseService


class Fetcher(BaseService):
    """
    fetch suffix sales from erp and put them into data warehouse
    """

    def __init__(self):
        super().__init__()

    def fetch(self, date_flag, begin_date, end_date):
        sql = 'oauth_saleTrendy @dateFlag=%s, @beginDate=%s, @endDate=%s'
        self.cur.execute(sql, (date_flag, begin_date, end_date))
        ret = self.cur.fetchall()
        for row in ret:
            yield (row['suffix'], row['orderTime'], float(row['amt']), row['dateFlag'])

    def push(self, rows):
        sql = 'insert into cache_suffixSales(suffix,orderTime,amt,dateFlag) values(%s,%s,%s, %s)'
        self.warehouse_cur.executemany(sql, list(rows))
        self.warehouse_con.commit()

    def work(self):
        try:
            rows = self.fetch(1, '2018-10-01', '2018-10-10')
            self.push(rows)
            self.logger.info('success to fetch suffix sales')
        except Exception as why:
            self.logger.error('fail to fetch suffix sales cause of {}'.format(why))
        finally:
            self.close()


if __name__ == '__main__':
    worker = Fetcher()
    worker.work()
