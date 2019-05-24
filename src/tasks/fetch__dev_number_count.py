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

    def fetch(self):
        sql = ("select salerName as developer, count(nid) as hasNumber "
               "from b_goods(nolock)  where goodsStatus not like '%清仓%' "
               " and isnull(salerName,'') != '' "
               "group by salerName")
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield (row['developer'],
                   row['hasNumber']
                   )

    def push(self, rows):
        sql = ('insert into cache_devHasNumber('
               'developer, hasNumber, updateTime)'
               'values(%s,%s,now())'
                ' ON DUPLICATE KEY UPDATE hasNumber=values(hasNumber),updateTime=now()'
               )
        self.warehouse_cur.executemany(sql, list(rows))
        self.warehouse_con.commit()

    def clean(self):
        pass

    def work(self):
        try:
            rows = self.fetch()
            self.push(rows)
            self.logger.info('success to fetch dev has number')
        except Exception as why:
            self.logger.error('fail to fetch dev has number of {}'.format(why))
        finally:
            self.close()


if __name__ == '__main__':
    worker = Fetcher()
    worker.work()
