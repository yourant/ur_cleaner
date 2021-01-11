#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-11-08 13:04
# Author: turpure

import os
import datetime
from src.services.base_service import CommonService


class SuffixSkuProfitFetcher(CommonService):
    """
    fetch suffix profit from erp day by day
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
        sql = 'EXEC guest.oauth_reportSuffixSkuProfitBackup @dateFlag=%s, @beginDate=%s, @endDate=%s'
        self.cur.execute(sql, (date_flag, begin_date, end_date))
        ret = self.cur.fetchall()

        for row in ret:
            yield (
                row['dateFlag'], row['orderDate'],row['suffix'], row['pingtai'], row['goodsCode'], row['goodsName'],
                row['storeName'], row['salerName'], row['devDate'], row['skuQty'], row['saleMoneyRmb'], row['refund'],row['profitRmb']
            )

    def push(self, rows):
        sql = ['insert into cache_suffixSkuProfitReport(',
               'dateFlag,orderDate,suffix,pingtai,goodsCode,goodsName,',
               'storeName,salerName,devDate,skuQty,saleMoneyRmb,refund,profitRmb',
               ') values (',
               '%s,%s,%s,%s,%s,%s,',
               '%s,%s,%s,%s,%s,%s,%s',
               ') ON DUPLICATE KEY UPDATE pingtai=values(pingtai),'
               'goodsName=values(goodsName),salerName=values(salerName),devDate=values(devDate),'
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
        self.logger.info(f'success to clear suffix sku profit between {begin} and {end}')

    # 根据cache_goods 表更新开发员 2020-06-11 添加
    def update_developer(self):
        sql = "UPDATE cache_suffixSkuProfitReport ss,cache_goods g SET ss.salerName=g.developer " \
              "WHERE g.goodsCode=ss.goodsCode AND ss.salerName <> g.developer"
        self.warehouse_cur.execute(sql)
        self.warehouse_con.commit()
        self.logger.info('success to update developer')

    def work(self):
        try:
            yesterday = str(datetime.datetime.today() - datetime.timedelta(days=1))[:10]
            today = str(datetime.datetime.today())[:10]
            last_month = (datetime.date.today().replace(day=1) - datetime.timedelta(days=1)).strftime("%Y-%m")
            last_month_first_day = str(last_month + '-01')
            # last_month_first_day = '2020-12-01'
            # today = '2021-01-06'
            self.clear(last_month_first_day, today)
            for date_flag in (0, 1):
                rows = self.fetch(date_flag, last_month_first_day, today)
                self.push(rows)

            self.update_developer()
        except Exception as why:
            self.logger.error('fail to fetch suffix sku profit cause of {}'.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = SuffixSkuProfitFetcher()
    worker.work()
