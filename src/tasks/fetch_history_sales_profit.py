#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-30 15:07
# Author: turpure

import os
import datetime
from src.services.base_service import CommonService


class Fetcher(CommonService):
    """
    fetch developer sold detail from erp and put them into data warehouse
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

    def fetch(self, exchange_rate, month):
        sql = 'call report_historyProfit (%s, %s)'
        self.warehouse_cur.execute(sql, (exchange_rate, month))
        ret = self.warehouse_cur.fetchall()
        for row in ret:
            yield (row['username'],
                   row['department'],
                   row['plat'],
                   row['monthName'],
                   row['hireDate'],
                   row['profit'] if row['profit'] else 0,
                   row['avgProfit'] if row['avgProfit'] else 0,
                   row['rank'] if row['rank'] else 0,
                   row['departmentTotal'] if row['departmentTotal'] else 0,
                   )

    def push(self, rows):
        sql = ('insert into cache_historySalesProfit('
               'username,department,plat,monthName,hireDate,profit,avgProfit,rank,departmentTotal)'
               'values(%s,%s,%s,%s,%s,%s,%s,%s,%s)'
                ' ON DUPLICATE KEY UPDATE profit=values(profit),'
               'hireDate=values(hireDate), avgProfit=values(avgProfit), rank=values(rank), departmentTotal=values(departmentTotal)'
               )
        self.warehouse_cur.executemany(sql, list(rows))
        self.warehouse_con.commit()

    def get_exchange(self):
        sql = 'SELECT salerRate FROM Y_RateManagement'
        self.cur.execute(sql)
        ret = self.cur.fetchone()
        return ret['salerRate']

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

    def get_suffix(self):
        self.warehouse_cur.execute('TRUNCATE TABLE `cache_suffix`;')
        self.warehouse_con.commit()

        sql = "SELECT DictionaryName AS suffix,case when FitCode='SMT' then 'Aliexpress' else FitCode end AS plat FROM [dbo].[B_Dictionary] WHERE CategoryID=12 AND Used=0 ORDER BY DictionaryName;"
        self.cur.execute(sql)
        ret = self.cur.fetchall()

        for row in ret:
            # yield (row['suffix'], row['plat'])

            item = (row['suffix'], row['plat'])
            insertSql = 'insert into cache_suffix(suffix,plat) values(%s,%s) '

            self.warehouse_cur.execute(insertSql, item)
            self.warehouse_con.commit()


    def work(self):
        try:
            self.get_suffix()

            exchange_rate = self.get_exchange()
            today = str(datetime.datetime.now())[:10]
            for month in self.get_month('2015-01-01', today):
                rows = self.fetch(exchange_rate, month)
                self.push(rows)
                self.logger.info('success to fetch dev goods profit details')
        except Exception as why:
            self.logger.error('fail to fetch dev goods profit details of {}'.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:

            self.close()


if __name__ == '__main__':
    worker = Fetcher()
    worker.work()
