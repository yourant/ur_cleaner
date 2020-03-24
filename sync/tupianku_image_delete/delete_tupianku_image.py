#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-10-19 14:23
# Author: turpure


import asyncio
import datetime

import aiohttp
from sync.tupianku_image_delete.image_server import BaseSpider
from pymongo import MongoClient
import random

class Worker(BaseSpider):

    def __init__(self, tupianku_name=2):
        super().__init__()
        self.tupianku_name = tupianku_name
        self.mongo = MongoClient('192.168.0.150', 27017)
        self.mongodb = self.mongo['ur_cleaner']
        self.col = self.mongodb['delete_tupianku_tasks']

    def _get_goods(self):
        sql = ("SELECT DISTINCT b.goodsCode FROM [dbo].[B_GoodsSKU]  bs LEFT JOIN B_Goods  b ON bs.GoodsID = b.NID" +
            " WHERE GoodsSKUStatus='停售' AND GoodsID NOT IN (SELECT DISTINCT GoodsID FROM [dbo].[B_GoodsSKU] WHERE GoodsSKUStatus<>'停售')")
        self.cur.execute(sql)
        goods_list = self.cur.fetchall()
        for row in goods_list:
            row['_id'] = row['goodsCode']
            row['tupianku1'] = 0
            row['tupianku2'] = 0
            yield row

    def push_tasks(self):
        goods_list = self._get_goods()
        for goods in goods_list:
            try:
                self.col.insert_one(goods)
                self.logger.info(f'success push task of {goods["goodsCode"]}')
            except Exception as why:
                self.logger.error(f'failed to push task of {goods["goodsCode"]} cause of {why}')

    def pull_tasks(self):
        tupianku_name = self.tupianku_name
        try:
            tasks = self.col.find({f'tupianku{tupianku_name}': 0}).limit(3000)
            return tasks
        except Exception as why:
            self.logger.error(f'failed to pull tasks of tupianku{tupianku_name} cause of {why}')

    async def delete_trans(self, goods_code, sema):
        try:
            async with sema:
                try:
                    #搜索图片，并获取图片id
                    image_ids = await self.search_image(goods_code + '-')
                    #删除图片
                    if image_ids:
                        await self.delete_image(goods_code, image_ids)
                    # 标记删除成功
                    await self.mark_as_done(goods_code)
                except Exception as why:
                    await self.login()
                    self.logger.error(f'error while delete image of goodsCode "{goods_code}" cause of {why}')
        except Exception as why:
            await self.login()
            self.logger.error(f'error while delete image of goodsCode "{goods_code}" cause of {why}')

    async def start(self, sema):
        tasks = self.pull_tasks()
        jobs = []
        await self.login()
        for row in tasks:
            jobs.append(asyncio.ensure_future(self.delete_trans(row['goodsCode'], sema)))
        await asyncio.wait(jobs)
        await self.session.close()

    def run(self):
        loop = asyncio.get_event_loop()
        sema = asyncio.Semaphore(20)
        try:
            loop.run_until_complete(self.start(sema))
        except Exception as why:
            self.logger.error(f'fail to delete image cause of {why}')
        finally:
            loop.close()
            self.mongo.close()

    async def mark_as_done(self, goodsCode):
        self.col.find_one_and_update({'_id': goodsCode},
                                     {'$set': {f'tupianku{self.tupianku_name}': 1,f'tupianku{self.tupianku_name}UpdatedTime':datetime.datetime.now()}})
        self.logger.info(f'mark {goodsCode}')


if __name__ == '__main__':
    import time
    start = time.time()
    worker = Worker(1)
    worker.run()
    end = time.time()
    date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
    print(date + f' it takes {end - start} seconds')