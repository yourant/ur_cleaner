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
        sql = 'call report_devGoodsProfit (%s, %s, %s)'
        self.warehouse_cur.execute(sql, (begin_date, end_date, date_flag))
        ret = self.warehouse_cur.fetchall()
        for row in ret:
            yield (row['developer'],
                   row['goodsCode'],
                   row['developDate'],
                   row['goodsStatus'],
                   row['sold'] if row['sold'] else 0,
                   row['amt'] if row['amt'] else 0,
                   row['profit'] if row['profit'] else 0,
                   row['rate'],
                   row['ebaySold'] if row['ebaySold'] else 0,
                   row['ebayProfit'] if row['ebayProfit'] else 0,
                   row['wishSold'] if row['wishSold'] else 0,
                   row['wishProfit'] if row['wishProfit'] else 0,
                   row['smtSold'] if row['smtSold'] else 0,
                   row['smtProfit'] if row['smtProfit'] else 0,
                   row['joomSold'] if row['joomSold'] else 0,
                   row['joomProfit'] if row['joomProfit'] else 0,
                   row['amazonSold'] if row['amazonSold'] else 0,
                   row['amazonProfit'] if row['amazonProfit'] else 0,
                   row['dateFlag'],
                   row['orderTime']
                   )

    def push(self, rows):
        sql = ('insert into cache_devGoodsProfit('
               'developer,goodsCode,devDate,goodsStatus,sold,amt,profit,rate,ebaySold,ebayProfit,wishSold,wishProfit,'
               'smtSold,smtProfit,joomSold,joomProfit,amazonSold,amazonProfit,dateFlag,orderTime)'
               'values(%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                ' ON DUPLICATE KEY UPDATE sold=values(sold),amt=values(amt),profit=values(profit),'
               'rate=values(rate),ebaySold=values(ebaySold),ebayProfit=values(ebayProfit),wishSold=values(wishSold),'
               'wishProfit=values(wishProfit),smtSold=values(smtSold),joomSold=values(joomSold),joomProfit=values(joomProfit),'
               'amazonSold=values(amazonSold),amazonProfit=values(amazonProfit)'
               )
        self.warehouse_cur.executemany(sql, list(rows))
        self.warehouse_con.commit()

    def clean(self):
        pass

    def work(self):
        try:
            today = str(datetime.datetime.today())[:10]
            four_days_ago = str(datetime.datetime.today() - datetime.timedelta(days=4))[:10]
            for date_flag in [0, 1]:
                rows = self.fetch(date_flag, four_days_ago, today)
                self.push(rows)
                self.logger.info('success to fetch dev goods profit details')
        except Exception as why:
            self.logger.error('fail to fetch dev goods profit details of {}'.format(why))
        finally:
            self.close()


if __name__ == '__main__':
    worker = Fetcher()
    worker.work()
