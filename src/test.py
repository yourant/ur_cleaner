#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-05-20 15:07
# Author: turpure

import datetime
from src.services.base_service import BaseService


class Fetcher(BaseService):
    """
    fetch developer sold detail from erp and put them into data warehouse
    """

    def __init__(self):
        super().__init__()

    def fetch(self):
        sql = ("select GoodsCode,SupplierName,Purchaser,SalerName, "
            " CASE WHEN c.CategoryparentName='全部类别' THEN c.CategoryName ELSE c.CategoryparentName END AS cate,"
            " CASE WHEN c.CategoryparentName='全部类别' THEN '' ELSE c.CategoryName END AS subCate "
            " from B_Goods g LEFT JOIN B_Supplier s ON g.SupplierID=s.NID "
            " LEFT JOIN B_GoodsCats c ON g.categorycode=c.categorycode WHERE Purchaser='王婉婷';")
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row['GoodsCode'], row['Purchaser'], row['SupplierName'], row['SalerName'],row['cate'],row['subCate']

    def push(self, rows):
        sql = 'insert into cache_goods_test_henry_20200527 (goodsCode,Purchaser,SupplierName, SalerName,cate,subCate) values (%s,%s,%s,%s,%s,%s)'
        self.warehouse_cur.executemany(sql, rows)
        self.warehouse_con.commit()

    def clean(self):
        sql = 'truncate table cache_goods_test_henry_20200527'
        self.warehouse_cur.execute(sql)
        self.warehouse_con.commit()

    def work(self):
        try:
            self.clean()
            rows = self.fetch()
            self.push(rows)
            self.logger.info('success to fetch  goods')
        except Exception as why:
            self.logger.error(f'fail to fetch goods cause of {why}')
        finally:
            self.close()


if __name__ == '__main__':
    worker = Fetcher()
    worker.work()
