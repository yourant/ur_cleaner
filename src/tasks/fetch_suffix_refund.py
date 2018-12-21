#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-12-13 13:46
# Author: turpure

import datetime
from src.services.base_service import BaseService


class RefundFetcher(BaseService):
    """
    fetch refund detail and put it into dw
    """

    def __init__(self):
        super().__init__()

    def fetch(self, begin_date, end_date):
        sql = 'oauth_saleRefund @beginDate=%s,@endDate=%s'
        self.cur.execute(sql, (begin_date, end_date))
        ret = self.cur.fetchall()
        for row in ret:
            yield (
                row['suffix'], row['goodsname'], row['goodscode'], row['sku'],
                row['tradeNid'], row['ack'], row['storename'],
                row['total_value'], row['currencyCode'],
                row['id'], row['refund_time']
            )

    def push(self, rows):
        sql = ("insert into cache_refund_details("
               "suffix,goodsName,goodsCode,goodsSku,tradeId,"
               "orderId,storeName,refund,currencyCode,refundId,"
               "refundTime) values ("
               "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")

        try:
            self.warehouse_cur.executemany(sql, rows)
            self.warehouse_con.commit()
            self.logger.info('success to fetch refund detail')
        except Exception as why:
            self.logger.error('fail to fetch refund detail case of {}'.format(why))

    def work(self):
        try:
            yesterday = str(datetime.datetime.today() - datetime.timedelta(days=1))[:10]
            month_first_day = str(datetime.datetime.strptime(yesterday[:8] + '01', '%Y-%m-%d'))[:10]
            rows = self.fetch(month_first_day, yesterday)
            self.push(rows)
        except Exception as why:
            self.logger.error('fail to fetch refund detail cause of {}'.format(why))
        finally:
            self.close()


if __name__ == '__main__':
    worker = RefundFetcher()
    worker.work()
