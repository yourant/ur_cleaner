#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-04-11 13:04
# Author: turpure

import datetime
from src.services.base_service import BaseService


class Worker(BaseService):
    """
    fetch ebay listing from ibay day by day
    """

    def __init__(self):
        super().__init__()

    def fetch(self) :
        sql = "select itemid,selleruserid, primarycategory, sku,site,listingstatus from ebay_item where listingstatus='Active'"
        self.ibay_cur.execute(sql)
        ret = self.ibay_cur.fetchall()
        for row in ret:
            # row
            yield (
                # row['itemid'], row['selleruserid'], row['primarycategory'], row['sku'], row['site'], row['itemtitle'], row['listingstatus']
                row[0],row[1],row[2],row[3],row[4],row[5]
            )

    def clear(self):
        sql = 'truncate table ebay_listing'
        self.ibay_cur.execute(sql)
        self.logger('success to clear ebay_listing')

    def push(self, rows):
        sql = ['insert into ebay_listing(',
               'itemid,selleruserid, primarycategory, sku,site,listingstatus ',
               ') values (',
               '%s,%s,%s,%s,%s,%s',
               ') '
               ]

        self.warehouse_cur.executemany(''.join(sql), rows)
        # for rw in rows:
        #     try:
        #         self.warehouse_cur.execute(''.join(sql), rw)
        #     except Exception as why:
        #         print(rw)
        #         rw[5] = ''
        #         self.warehouse_cur.execute(''.join(sql), rw)
        self.warehouse_con.commit()
        self.logger.info('success to fetch suffix profit')

    def work(self):
        try:
            today = str(datetime.datetime.today())[:10]
            rows = self.fetch()
            self.push(rows)
        except Exception as why:
            self.logger.error('fail to fetch ebay_listing cause of {}'.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()
