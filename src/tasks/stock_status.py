#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-03-14 10:07
# Author: turpure

import time
from src.services.base_service import BaseService

"""
库存情况。
"""


class Worker(BaseService):

    def __init__(self):
        super().__init__()

    def get_stock_waring_info(self):
        sql = "EXEC oauth_stockStatus 1,1,1000000;"
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield (row['goodsCode'], row['sku'], row['skuName'], row['storeName'], row['goodsStatus'], row['salerName'],
                   row['createDate'], row['costPrice'], row['useNum'], row['costmoney'], row['notInStore'],
                   row['notInCostmoney'], row['hopeUseNum'], row['totalCostmoney'], row['sellCount1'],
                   row['sellCount2'], row['sellCount3'], row['weight'], row['updateTime'])

    def push(self, rows):
        sql = ("insert into cache_stockWaringTmpData(goodsCode, sku, skuName, storeName, goodsStatus, "
               "salerName, createDate, costPrice, useNum, costmoney,notInStore, notInCostmoney, "
               "hopeUseNum, totalCostmoney, sellCount1, sellCount2, sellCount3, weight, updateTime) "
               "values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ")
        try:
            self.warehouse_cur.executemany(sql, list(rows))
            self.warehouse_con.commit()
            self.logger.info('success to get stock waring info')
        except Exception as why:
            self.logger.error('failed to get stock waring info cause of %s' % why)

    def get_30days_order_info(self):
        sql = "EXEC oauth_stockStatus;"
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield (row['sku'], row['salerName'], row['storeName'], row['goodsStatus'], row['costMoney'],
                   row['updateTime'], row['threeSellCount'], row['sevenSellCount'], row['fourteenSellCount'],
                   row['thirtySellCount'], row['trend'])

    def insert(self, rows):
        sql = ("insert into cache_30DayOrderTmpData(sku, salerName, storeName, goodsStatus, costMoney, updateTime, "
               "threeSellCount, sevenSellCount, fourteenSellCount, thirtySellCount, trend) "
               "values( %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
        try:
            self.warehouse_cur.executemany(sql, list(rows))
            self.warehouse_con.commit()
            self.logger.info('success to get 30 days orders info')
        except Exception as why:
            self.logger.error('failed to get 30 days orders info cause of %s' % why)

    def clean(self):
        sql = "TRUNCATE TABLE cache_stockWaringTmpData;"
        sql2 = "TRUNCATE TABLE cache_30DayOrderTmpData;"
        self.warehouse_cur.execute(sql)
        self.warehouse_cur.execute(sql2)
        self.warehouse_con.commit()
        self.logger.info('success to clean table')

    def run(self):
        begin_time = time.time()
        try:
            self.clean()
            tasks = self.get_stock_waring_info()
            self.push(tasks)
            orders = self.get_30days_order_info()
            self.insert(orders)
        except Exception as why:
            self.logger.error(why)
        finally:
            self.close()
        print('程序耗时{:.2f}'.format(time.time() - begin_time))  # 计算程序总耗时


if __name__ == '__main__':
    worker = Worker()
    worker.run()
