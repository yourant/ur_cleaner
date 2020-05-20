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
        sql = 'select  goodsCode, goodsName, goodsStatus, BmpFileName as img from b_goods(nolock)'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row['goodsCode'], row['goodsName'], row['goodsStatus'], row['img']

    def push(self, rows):
        sql = 'insert into cache_goods (goodsCode,goodsName,goodsStatus, img) values (%s,%s,%s,%s)'
        self.warehouse_cur.executemany(sql, rows)
        self.warehouse_con.commit()

    def clean(self):
        sql = 'truncate table cache_goods'
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
