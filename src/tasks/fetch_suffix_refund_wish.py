#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-10-09 13:46
# Author: turpure

import os
import datetime
from src.services.base_service import CommonService


class WishRefundFetcher(CommonService):
    """
    fetch refund detail and put it into dw
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

    def fetch(self, begin_date, end_date):
        sql = 'oauth_saleRefundWish @beginDate=%s,@endDate=%s'
        self.cur.execute(sql, (begin_date, end_date))
        ret = self.cur.fetchall()
        for row in ret:
            yield (
                row['suffix'], row['goodsname'], row['goodscode'], row['sku'],
                row['tradeNid'], row['mergeBillId'], row['ack'], row['storename'],
                row['total_value'], row['currencyCode'],
                row['id'], row['refund_time'], row['orderTime'], row['orderCountry'],
                row['platform'], row['expressWay'], row['refMonth'], row['dateDelta']
            )

    def push(self, rows):
        sql = ("insert into cache_refund_details_wish("
               "suffix,goodsName,goodsCode,goodsSku,tradeId,mergeBillId,"
               "orderId,storeName,refund,currencyCode,refundId,"
               "refundTime,orderTime,orderCountry,platform,expressWay,refMonth,dateDelta) values ("
               "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
               "ON DUPLICATE KEY UPDATE refund=values(refund), mergeBillId=values(mergeBillId),"
               "orderTime=values(orderTime),orderCountry=values(orderCountry),"
               "platform=values(platform),expressWay=values(expressWay),"
               "refMonth=values(refMonth),dateDelta=values(dateDelta)")

        try:
            self.warehouse_cur.executemany(sql, rows)
            self.warehouse_con.commit()
            self.logger.info('success to fetch wish refund detail')
        except Exception as why:
            self.logger.error('fail to fetch wish refund detail case of {}'.format(why))

    def clear(self, begin, end):
        sql = 'delete  from cache_refund_details_wish where refundTime between %s and %s '
        self.warehouse_cur.execute(sql, (begin, end))
        self.warehouse_con.commit()
        self.logger.info('success to clear wish refund data between {} and {}'.format(begin, end))

    def work(self, begin, end):
        try:
            self.clear(begin, end)
            rows = self.fetch(begin, end)
            self.push(rows)
        except Exception as why:
            self.logger.error('fail to fetch wish refund detail cause of {}'.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == '__main__':
    yesterday = str(datetime.datetime.today() - datetime.timedelta(days=1))[:10]
    today = str(datetime.datetime.today())[:10]
    month_first_day = str(datetime.datetime.strptime(yesterday[:8] + '01', '%Y-%m-%d'))[:10]
    # month_first_day = '2020-09-01'
    worker = WishRefundFetcher()
    worker.work(month_first_day, today)
