#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-04-11 13:04
# Author: turpure

import time
import math
import asyncio
from src.services.base_service import BaseService


class Worker(BaseService):
    """
    fetch ebay listing from ibay day by day
    """

    def __init__(self):
        super().__init__()
        self.step = 10000

    async def fetch(self, sema, i) :
        sql = ("SELECT itemid,sku,inventory, " +
                " (CASE WHEN strpos(sku,'*') > 0 THEN substring(sku,1,strpos(sku,'*') - 1) " +
                " WHEN strpos(sku,'@') > 0 THEN substring(sku,1,strpos(sku,'@') - 1) " +
                " WHEN strpos(sku,'#') > 0 THEN substring(sku,1,strpos(sku,'#') - 1) " +
                " ELSE sku END) AS newSku " +
                " FROM wish_item_variation_specifics wi " +
                " WHERE enabled='True' AND inventory>=0 " +
                "-- AND EXISTS (SELECT * FROM wish_item w " +
                "-- INNER JOIN aliexpress_user u ON u.selleruserid=w.selleruserid " +
                "-- WHERE w.itemid=wi.itemid AND w.is_promoted = 0 AND listingstatus='Active' AND u.platform='wish' AND u.state1=1 ) " +
                "AND id BETWEEN %s AND %s")
        try:
            async with sema:
                self.ibay_cur.execute(sql, (i * self.step + 1, (i + 1) * self.step))
                ret = self.ibay_cur.fetchall()
                if ret:
                    self.push(ret)
        except Exception as error:
            self.logger.error(f'fail to fetch wish listing cause of {error}')
        # for row in ret:
        #     yield (row[0],row[1],row[2],row[3])


    def get_max_id(self) :
        sql = "SELECT MAX(id) as maxId FROM wish_item_variation_specifics WHERE enabled='True' AND inventory>=0"
        self.ibay_cur.execute(sql)
        ret = self.ibay_cur.fetchone()
        return ret

    def clear(self):
        sql = 'truncate table ibay365_wish_listing'
        self.cur.execute(sql)
        self.logger.info('success to clear ibay365_wish_listing')

    def push(self, rows):
        sql = 'insert into ibay365_wish_listing(itemid, sku, inventory, newSku) values (%s,%s,%s,%s)'
        self.cur.executemany(''.join(sql), rows)
        self.con.commit()
        self.logger.info('success to fetch wish listing')


    def run(self):
        try:
            self.clear()
            maxId = self.get_max_id()
            total = math.ceil(maxId[0] / self.step)
            sema = asyncio.Semaphore(100)
            loop = asyncio.get_event_loop()
            tasks = [asyncio.ensure_future(self.fetch(sema, i )) for i in range(total + 1) ]
            loop.run_until_complete(asyncio.wait(tasks))
            loop.close()

        except Exception as why:
            self.logger.error(f'fail to fetch ibay365_wish_listing because of {why}')
        finally:
            self.close()


if __name__ == "__main__":
    start = time.time()
    worker = Worker()
    worker.run()
    end = time.time()
    print(f'it takes {end - start} seconds')