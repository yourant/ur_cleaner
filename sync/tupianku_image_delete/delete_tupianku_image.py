#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-10-19 14:23
# Author: turpure


import asyncio
import aiohttp
from sync.tupianku_image_delete.image_server import BaseSpider


class Worker(BaseSpider):

    def __init__(self):
        super().__init__()

    async def get_goods(self):
        sql = ("SELECT DISTINCT b.goodsCode FROM [dbo].[B_GoodsSKU]  bs LEFT JOIN B_Goods  b ON bs.GoodsID = b.NID" +
            " WHERE GoodsSKUStatus='停售' AND GoodsID NOT IN (SELECT DISTINCT GoodsID FROM [dbo].[B_GoodsSKU] WHERE GoodsSKUStatus<>'停售')" +
            " AND  GoodsCode  NOT IN (SELECT DISTINCT GoodsCode FROM [dbo].[TPK_goodsCode_del])" )
        self.cur.execute(sql)
        goods_list = self.cur.fetchall()
        # print(goods_list)
        return goods_list

    async def deal(self, goodsCode):
        async with aiohttp.ClientSession() as session:
            try:
                #登录图片库
                login_response = await self.log_in(session)
                login_html = await login_response.text()

                #搜索图片，并获取图片id
                search_response = await self.search_image(session, goodsCode)
                search_html = await search_response.text()
                # print(search_html)
                image_ids = self.get_image_ids(search_html)
                # print(image_ids)

                #删除图片
                if image_ids:
                    await self.delete_image(session, image_ids)

            except Exception as why:
                self.logger.error(f'error while delete image of goodsCode "{goodsCode}" cause of {why}')




    async def save(self, goodsCode):
        sql = "insert into dbo.tpk_goodsCode_del(goodsCode, updateDate) values (%s,%s)"
        updateDate = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        self.cur.execute(sql,(goodsCode, updateDate))
        self.con.commit()


if __name__ == '__main__':
    import time
    start = time.time()
    worker = Worker()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(worker.run())
    end = time.time()
    date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
    print(date + f' it takes {end - start} seconds')

