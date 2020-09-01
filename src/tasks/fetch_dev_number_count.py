#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-30 15:07
# Author: turpure

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

    def fetch(self):
        sql = ("select salerName as developer, count(nid) as hasNumber "
               "from b_goods(nolock)  where goodsStatus in ('爆款', '旺款', '浮动款', '在售') "
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
