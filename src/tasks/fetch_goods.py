#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-05-20 15:07
# Author: turpure

import os
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

    def fetch(self):
        sql = ("select  goodsCode, goodsName, salerName, goodsStatus, ISNULL(devDate,createDate) as devDate, " +
               "(select top 1 BmpFileName from b_goodsSku(nolock) where goodsId=bg.nid) as img, " +
               "CASE WHEN ISNULL(possessMan2,'')='' THEN '王伟' ELSE possessMan2 END AS seller " +
               "from b_goods(nolock) as bg")
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield (row['goodsCode'], row['goodsName'], row['salerName'], row['goodsStatus'], row['devDate'], row['img'],
                   row['seller'])

    def push(self, rows):
        sql = ('insert into cache_goods(goodsCode,goodsName,developer,goodsStatus,devDate,img,seller)' +
               ' values (%s,%s,%s,%s,%s,%s,%s)')
        self.warehouse_cur.executemany(sql, rows)
        self.warehouse_con.commit()

    def clean(self):
        sql = 'truncate table cache_goods'
        self.warehouse_cur.execute(sql)
        self.warehouse_con.commit()

    def update_sku_seller(self):
        sql = (" INSERT INTO cache_skuSeller(goodsCode,seller1,updateDate) " +
               "SELECT goodsCode,'王伟' AS seller1,NOW() AS updateDate FROM cache_goods g " +
               "WHERE  (g.goodsCode  LIKE 'UK-%' OR g.goodsCode  LIKE 'GC-%') " +
               "AND NOT EXISTS(SELECT * FROM cache_skuSeller s WHERE s.goodsCode=g.goodsCode)")
        self.warehouse_cur.execute(sql)
        self.warehouse_con.commit()

    def work(self):
        try:
            self.clean()
            rows = self.fetch()
            self.push(rows)
            self.update_sku_seller()
            self.logger.info('success to fetch  goods')
        except Exception as why:
            self.logger.error(f'fail to fetch goods cause of {why}')
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == '__main__':
    worker = Fetcher()
    worker.work()
