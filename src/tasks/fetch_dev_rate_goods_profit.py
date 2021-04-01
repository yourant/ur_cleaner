#! usr/bin/env/python3
# coding:utf-8
# @Time: 2021-03-20 13:04
# Author: turpure

import os
import datetime
from src.services.base_service import CommonService


class DevRateGoodsFetcher(CommonService):
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
        sql = 'EXEC oauth_devRateGoodsProfit  @dateFlag=%s, @beginDate=%s, @endDate=%s'
        self.cur.execute(sql, (date_flag, begin_date, end_date))
        ret = self.cur.fetchall()

        for row in ret:
            yield (
                row['orderTime'], row['dateFlag'], row['SalerName'], row['goodsCode'], row['img'], row['SaleMoneyRmbUS'],
                row['SaleMoneyRmbZn'], row['CostMoneyRmb'], row['PPebayUS'], row['PPebayZn'], row['InPackageFeeRmb'],
                row['ExpressFareRmb'], row['NetProfit'], row['netRate'], row['sold']
            )

    def push(self, rows):
        sql = ['insert into cache_devRateGoodsProfit(',
               'ordertime , dateFlag , salerName, goodsCode, img,saleMoneyRmbUS , saleMoneyRmbZn, costMoneyRmb , PPebayUS ,'
               ' PPebayZn , inPackageFeeRmb , expressFareRmb , netProfit , netRate, sold',
               ') values (',
               '%s,%s,%s,%s,%s,%s,%s,',
               '%s,%s,%s,%s,%s,%s,%s,%s',
               ') on duplicate key update sold=values(sold),saleMoneyRmbUS=values(saleMoneyRmbUS), '
               'costMoneyRmb=values(costMoneyRmb) , PPebayUS=values(PPebayUS) ,'
               ' inPackageFeeRmb=values(inPackageFeeRmb) , expressFareRmb=values(expressFareRmb)'
               ]
        self.warehouse_cur.executemany(''.join(sql), rows)
        self.warehouse_con.commit()
        self.logger.info('success to fetch cache_devRateGoodsProfit')

    def clear(self, begin, end):
        sql = 'delete from cache_devRateGoodsProfit where orderDate BETWEEN %s and  %s'
        self.warehouse_cur.execute(sql, (begin, end))
        self.warehouse_con.commit()
        self.logger.info(f'success to clear cache_devRateGoodsProfit between {begin} and {end}')

    def work(self):
        try:
            today = str(datetime.datetime.today())[:10]
            last_month = (datetime.date.today().replace(day=1) - datetime.timedelta(days=1)).strftime("%Y-%m")
            last_month_first_day = str(last_month + '-01')
            # last_month_first_day = '2021-01-01'
            # today = '2021-01-06'
            # self.clear(last_month_first_day, today)
            for date_flag in (0, 1):
                rows = self.fetch(date_flag, last_month_first_day, today)
                self.push(rows)
        except Exception as why:
            self.logger.error('fail to fetch cache_devRateGoodsProfit cause of {}'.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = DevRateGoodsFetcher()
    worker.work()
