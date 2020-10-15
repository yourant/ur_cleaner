#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-10-19 14:23
# Author: turpure


import asyncio
import datetime
import time

from sync.tupianku_image_delete.image_server import BaseSpider
from pymongo import MongoClient


class Worker(BaseSpider):

    def __init__(self, tupianku_name=2):
        super().__init__(tupianku_name)
        self.mongo = MongoClient('192.168.0.150', 27017)
        self.mongodb = self.mongo['ur_cleaner']
        self.col = self.mongodb['delete_tupianku_tasks']

    def _get_goods(self):
        sql = ("SELECT DISTINCT top 300 b.goodsCode FROM [dbo].[B_GoodsSKU]  bs LEFT JOIN B_Goods  b ON bs.GoodsID = b.NID" +
               " WHERE GoodsSKUStatus='停售' AND isnull(b.goodsCode,'')<>''" +
               " AND GoodsID NOT IN (SELECT DISTINCT GoodsID FROM [dbo].[B_GoodsSKU] WHERE GoodsSKUStatus<>'停售')")
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
                # self.col.insert_one(goods)
                self.col.update_one({'_id': goods['_id']}, {"$set": goods}, upsert=True)
                # self.logger.info(f'success to push task of {goods["goodsCode"]}')
            except Exception as why:
                pass
                # self.logger.error(f'failed to push task of {goods["goodsCode"]} cause of {why}')

        self.logger.info(f'success to push task of goods code to delete')

    def pull_tasks(self):
        tupianku_name = self.tupianku_name
        try:
            tasks = self.col.find({f'tupianku{tupianku_name}': 0}).limit(1000)
            return tasks
        except Exception as why:
            self.logger.error(f'failed to pull tasks of tupianku{tupianku_name} cause of {why}')

    async def delete_trans(self, goods_code, sema):
        async with sema:
            try:
                # 搜索图片，并获取图片id
                image_ids = await self.search_image(goods_code)
                # 删除图片
                if type(image_ids) is list:
                    if image_ids:
                        ret = await self.delete_image(goods_code, image_ids)
                        if ret:
                            await self.mark_as_done(goods_code)
                    # 标记删除成功
                    else:
                        await self.mark_as_done(goods_code)
            except Exception as why:
                await self.login()
                self.logger.error(f'error while delete image of goodsCode "{goods_code}" cause of {why}')

    async def start(self, sema):
        tasks = self.pull_tasks()
        jobs = []
        await self.login()
        for row in tasks:
            if row['goodsCode']:
                jobs.append(asyncio.ensure_future(self.delete_trans(row['goodsCode'], sema)))
        await asyncio.wait(jobs)
        await self.session.close()

    def run(self):
        loop = asyncio.get_event_loop()
        sema = asyncio.Semaphore(30)
        try:
            loop.run_until_complete(self.start(sema))
        except Exception as why:
            self.logger.error(f'fail to delete image cause of {why}')
        finally:
            loop.close()
            self.mongo.close()

    async def mark_as_done(self, goodsCode):
        self.col.find_one_and_update({'_id': goodsCode},
                                     {'$set': {f'tupianku{self.tupianku_name}': 1,
                                               f'tupianku{self.tupianku_name}UpdatedTime': datetime.datetime.now()}})
        self.logger.info(f'mark {goodsCode}')

    def work(self):
        start = time.time()
        try:
            # self.push_tasks()
            self.run()
        except Exception as why:
            self.logger.error(f'fail to finish task of  deleting images cause of {why}')
        finally:
            self.close()
            self.mongo.close()

        end = time.time()
        date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
        print(date + f' it takes {end - start} seconds')


if __name__ == '__main__':
    worker = Worker(1)
    worker.work()
