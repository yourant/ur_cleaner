#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure

import datetime
from src.services.base_service import BaseService


class SkuCounter(BaseService):
    """
    counter order number day by day
    """

    def __init__(self):
        super().__init__()

    def fetch(self, date_flag, begin_date, end_date):
        sql = 'oauth_skuCount @dateFlag=%s,@beginDate=%s,@endDate=%s'
        self.cur.execute(sql, (date_flag, begin_date, end_date))
        ret = self.cur.fetchall()
        for row in ret:
            yield (row['addressOwner'], row['suffix'], row['orderTime'], row['orderCount'], row['dateFlag'])

    def push(self, rows):
        sql = ["insert into cache_skuCount",
               "(addressOwner,suffix,orderTime,orderCount,dateFlag)",
               "values",
               "( %s,%s,%s,%s,%s)",
               "on duplicate key update orderCount=values(orderCount)"]
        self.warehouse_cur.executemany(''.join(sql), rows)
        self.warehouse_con.commit()
        self.logger.info('success to count sku')

    def work(self):
        try:
            today = str(datetime.datetime.today())[:10]
            some_day_ago = str(datetime.datetime.today() - datetime.timedelta(days=30))[:10]
            for i in (0, 1):
                res = self.fetch(i, some_day_ago, today)
                self.push(res)
        except Exception as why:
            self.logger.error('fail to count sku cause of {} '.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    worker = SkuCounter()
    worker.work()


