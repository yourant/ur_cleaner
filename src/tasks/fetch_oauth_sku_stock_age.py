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

    def fetch(self, flag):
        sql = "EXEC oauth_skuStorageAge '','','','','',0," + str(flag)
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            if flag == 0:
                yield (row['sku'], row['mainImage'], row['storeName'], row['skuName'], row['salerName'], row['season'],
                       row['goodsSkuStatus'], row['createDate'], row['number'], row['money'], row['cate'],
                       row['subCate'], row['thirtyStockNum'], row['thirtyStockMoney'], row['sixtyStockNum'],
                       row['sixtyStockMoney'], row['ninetyStockNum'], row['ninetyStockMoney'], row['moreStockNum'],
                       row['moreStockMoney'], row['updateTime'])
            else:
                yield row

    def push(self, rows):
        # for row in rows:
        #     print(row)
        sql = ('insert into cache_skuStockAge('
               'sku, mainImage, storeName, skuName, salerName, season, goodsSkuStatus, createDate, number, money, '
               'cate, subCate, thirtyStockNum, thirtyStockMoney, sixtyStockNum, sixtyStockMoney, ninetyStockNum,'
               'ninetyStockMoney, moreStockNum, moreStockMoney, updateTime)'
               'values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ')
        self.warehouse_cur.executemany(sql, rows)
        self.warehouse_con.commit()

    def update_one(self, row):
        update_time = str(datetime.datetime.today())[:19]
        sql = ('UPDATE cache_skuStockAge SET updateTime=%s, maxPurchaseDay=%s '
               'WHERE sku=%s AND storeName=%s')
        self.warehouse_cur.execute(sql, (update_time, row['maxPurchaseDay'], row['sku'], row['storeName']))
        self.warehouse_con.commit()

    def get_purchase_data(self, rows):
        flag = False
        item = dict()
        for row in rows:
            if row['order_id'] == 1:
                item = row
                flag = False
            if not flag:
                # 累计入库数量 大于等于 现有库存，取当前时间
                if item['inAmount'] >= row['number']:
                    if row['makeDate']:
                        max_purchase_day = (datetime.datetime.now() - row['makeDate']).days
                    else:
                        max_purchase_day = 180
                    res_item = {
                        'sku': item['sku'],
                         'storeName': item['storeName'],
                         'maxPurchaseDay': max_purchase_day
                        }
                    self.update_one(res_item)
                    # 设置标记，已取到最大日期，后面的同仓库同SKU数据，直接跳过
                    flag = True
                else:
                    item['inAmount'] = item['inAmount'] + row['inAmount']

            else:
                continue

    def clean(self):
        sql = 'DELETE from cache_skuStockAge'
        self.warehouse_cur.execute(sql)
        self.warehouse_con.commit()

    def work(self):
        try:
            self.clean()
            rows = self.fetch(flag=0)
            self.push(rows)
            # 更新最大 采购天数
            items = self.fetch(flag=1)
            self.get_purchase_data(items)
            self.logger.info('success to fetch sku stock age data!')
        except Exception as why:
            self.logger.error('fail to fetch dev goods profit details of {}'.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == '__main__':
    worker = Fetcher()
    worker.work()
