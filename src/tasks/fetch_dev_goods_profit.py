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

    def fetch(self, begin_date, end_date, date_flag):
        sql = 'call report_devGoodsProfit (%s, %s, %s)'
        self.warehouse_cur.execute(sql, (begin_date, end_date, date_flag))
        ret = self.warehouse_cur.fetchall()
        for row in ret:
            yield (row['developer'],
                   row['goodsCode'],
                   row['developDate'],
                   row['goodsStatus'],
                   row['sold'] if 'sold' in row else 0,
                   row['amt'] if 'amt' in row else 0,
                   row['profit'] if 'profit' in row else 0,
                   row['rate'] if 'rate' in row else 0,
                   row['ebaySold'] if 'ebaySold' in row else 0,
                   row['ebayProfit'] if 'ebayProfit' in row else 0,
                   row['wishSold'] if 'wishSold' in row else 0,
                   row['wishProfit'] if 'wishProfit' in row else 0,
                   row['smtSold'] if 'smtSold' in row else 0,
                   row['smtProfit'] if 'smtProfit' in row else 0,
                   row['joomSold'] if 'joomSold' in row else 0,
                   row['joomProfit'] if 'joomProfit' in row else 0,
                   row['amazonSold'] if 'amazonSold' in row else 0,
                   row['amazonProfit'] if 'amazonProfit' in row else 0,
                   row['vovaSold'] if 'vovaSold' in row else 0,
                   row['vovaProfit'] if 'vovaProfit' in row else 0,
                   row['lazadaSold'] if 'lazadaSold' in row else 0,
                   row['lazadaProfit'] if 'lazadaProfit' in row else 0,
                   row['dateFlag'] if 'dateFlag' in row else 0,
                   row['orderTime'] if 'orderTime' in row else ''
                   )

    def push(self, rows):
        # for row in rows:
        #     print(row)
        sql = ('insert into cache_devGoodsProfit('
               'developer,goodsCode,devDate,goodsStatus,sold,amt,profit,rate,ebaySold,ebayProfit,wishSold,wishProfit,'
               'smtSold,smtProfit,joomSold,joomProfit,amazonSold,amazonProfit,vovaSold,vovaProfit,'
               'lazadaSold,lazadaProfit,dateFlag,orderTime)'
               'values(%s,%s,%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '
                ' ON DUPLICATE KEY UPDATE goodsStatus=values(goodsStatus),sold=values(sold),amt=values(amt), '
               'profit=values(profit),rate=values(rate),ebaySold=values(ebaySold),ebayProfit=values(ebayProfit), '
               'wishSold=values(wishSold),wishProfit=values(wishProfit),'
               'smtSold=values(smtSold),smtProfit=values(smtProfit),'
               'joomSold=values(joomSold),joomProfit=values(joomProfit),'
               'amazonSold=values(amazonSold),amazonProfit=values(amazonProfit),'
               'vovaSold=values(vovaSold),vovaProfit=values(vovaProfit),'
               'lazadaSold=values(lazadaSold),lazadaProfit=values(lazadaProfit)'
               )
        self.warehouse_cur.executemany(sql, rows)
        self.warehouse_con.commit()

    def clean(self, begin, end):
        sql = 'DELETE from cache_devGoodsProfit where orderTime between %s and %s'
        self.warehouse_cur.execute(sql, (begin, end))
        self.warehouse_con.commit()

    @staticmethod
    def date_range(begin_date, end_date):
        dt = datetime.datetime.strptime(begin_date, "%Y-%m-%d")
        date = begin_date[:]
        while date <= end_date:
            dt = dt + datetime.timedelta(1)
            date = dt.strftime("%Y-%m-%d")
            yield date

    def get_month(self, begin_date, end_date):
        month = []
        for date in self.date_range(begin_date, end_date):
            if date[:7] not in month:
                month.append(date[:7])
        return month

    @staticmethod
    def next_month(month):
        (year, month) = month.split('-')
        month = int(month)
        year = int(year)
        if month >= 9 and month < 12:
            month += 1
        elif month >= 1 and month < 9:
            month += 1
            month = '0' + str(month)
        else:
            year += 1
            month = '01'
        return str(year) + '-' + str(month) + '-' + '01'

    def update_goods_status(self, end_date):
        today = datetime.datetime.now().day
        begin_date = str(datetime.datetime.today() - datetime.timedelta(days=60))[:10]
        if today == 5 or today == 15 or today == 25:
            sql = ('UPDATE cache_devGoodsProfit t ' +
                    'SET goodsStatus = (SELECT goodsStatus AS goodsStatus FROM cache_devGoodsSoldDetail d WHERE d.goodsCode = t.goodsCode order by orderTime desc limit 1)' +
                    'WHERE exists(select 1 from cache_devGoodsSoldDetail t2 where t2.goodsCode = t.goodsCode AND t2.goodsStatus = t.goodsStatus)' +
                    "AND orderTime BETWEEN %s AND %s;")
            self.warehouse_cur.execute(sql, (begin_date, end_date))
            self.warehouse_con.commit()

    def work(self):
        try:
            end_date = str(datetime.datetime.today() - datetime.timedelta(days=1))[:10]
            begin_date = str(datetime.datetime.today() - datetime.timedelta(days=30))[:10]
            self.clean(begin_date, end_date)
            self.update_goods_status(begin_date)
            for date_flag in [0, 1]:
                month = self.get_month(begin_date, end_date)
                for mon in month:
                    begin = mon + '-01'
                    end = self.next_month(mon)
                    rows = self.fetch(begin, end, date_flag)
                    self.push(rows)
                    self.logger.info('success to fetch dev goods profit details between {} and {}'.format(begin, end))
        except Exception as why:
            self.logger.error('fail to fetch dev goods profit details of {}'.format(why))
        finally:
            self.close()


if __name__ == '__main__':
    worker = Fetcher()
    worker.work()
