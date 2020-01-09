#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-11-08 13:04
# Author: turpure

import datetime
from src.services.base_service import BaseService


class SuffixSkuProfitFetcher(BaseService):
    """
    fetch suffix profit from erp day by day
    """

    def __init__(self):
        super().__init__()

    def fetch(self, date_flag, begin_date, end_date):
        sql = 'EXEC guest.oauth_reportSuffixSkuProfitBackup @dateFlag=%s, @beginDate=%s, @endDate=%s'
        self.cur.execute(sql, (date_flag, begin_date, end_date))
        ret = self.cur.fetchall()

        for row in ret:
            yield (
                row['dateFlag'], row['orderDate'],row['suffix'], row['pingtai'], row['goodsCode'], row['goodsName'],
                row['storeName'], row['salerName'], row['skuQty'], row['saleMoneyRmb'], row['refund'],row['profitRmb']
            )

    def push(self, rows):
        sql = ['insert into cache_suffixSkuProfitReport(',
               'dateFlag,orderDate,suffix,pingtai,goodsCode,goodsName,',
               'storeName,salerName,skuQty,saleMoneyRmb,refund,profitRmb',
               ') values (',
               '%s,%s,%s,%s,%s,%s,',
               '%s,%s,%s,%s,%s,%s',
               ') ON DUPLICATE KEY UPDATE pingtai=values(pingtai),'
               'goodsName=values(goodsName),salerName=values(salerName),'
               'skuQty=values(skuQty),saleMoneyRmb=values(saleMoneyRmb),'
               'refund=values(refund),profitRmb=values(profitRmb)'
               ]
        self.warehouse_cur.executemany(''.join(sql), rows)
        self.warehouse_con.commit()
        self.logger.info('success to fetch suffix sku profit')

    def clear(self, begin, end):
        sql = 'delete from cache_suffixSkuProfitReport where orderDate BETWEEN %s and  %s'
        self.warehouse_cur.execute(sql, (begin, end))
        self.warehouse_con.commit()
        self.logger.info('success to clear suffix sku profit')

    def work(self):
        try:
            yesterday = str(datetime.datetime.today() - datetime.timedelta(days=1))[:10]
            # yesterday = '2019-11-01'
            today = str(datetime.datetime.today())[:10]
            last_month = (datetime.date.today().replace(day=1) - datetime.timedelta(days=1)).strftime("%Y-%m")
            last_month_first_day = str(last_month + '-01')
            self.clear(last_month_first_day, today)
            for date_flag in (0, 1):
                rows = self.fetch(date_flag, last_month_first_day, today)
                self.push(rows)
        except Exception as why:
            self.logger.error('fail to fetch suffix sku profit cause of {}'.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    worker = SuffixSkuProfitFetcher()
    worker.work()
