#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-09-17 16:57
# Author: turpure

from src.services.base_service import BaseService
import aiohttp
import json
import asyncio
import datetime
from concurrent.futures import ThreadPoolExecutor


class Uploading(BaseService):
    def __init__(self):
        super().__init__()

    def clean(self):
        sql = 'truncate table ibay365_vova_list'
        self.cur.execute(sql)
        self.con.commit()
        self.logger.info('success to clear vova listing')

    def get_vova_token(self):

        sql = 'SELECT AliasName AS suffix,MerchantID AS selleruserid,APIKey AS token FROM [dbo].[S_SyncInfoVova] WHERE SyncInvertal=0;'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        return ret

    async def get_products(self, token):
        url = 'https://merchant.vova.com.hk/api/v1/product/productList'
        param = {
            "token": token['token'],
            "conditions": {
                "goods_state_filter": "",
                "middle_east_sale": "",
                "goods_promotion": "",
                "ban_status": "",
                "upload_batch_id": 0,
                "search": {
                    "search_type": "",
                    "search_value": ""
                },
                "page_arr": {
                    "perPage": 100,
                    "page": 1
                }
            }
        }
        async with aiohttp.ClientSession() as session:
            response = await session.post(url, data=json.dumps(param))
            ret = await response.json(content_type='application/json')
            total_page = ret['page_arr']['totalPage']
            rows = self.deal_products(token, ret['product_list'])
            #asyncio.gather(asyncio.ensure_future(self.save(rows, token, page=1)))
            asyncio.ensure_future(self.save(rows, token, page=1))
            self.save(rows, token, page=1)
            if total_page > 1:
                for page in range(2, total_page + 1):
                    param['conditions']['page_arr']['page'] = page
                    try:
                        response = await session.post(url, data=json.dumps(param))
                        res = await response.json()
                        res_data = self.deal_products(token, res['product_list'])
                        self.save(res_data,token, page)
                        asyncio.ensure_future(self.save(res_data, token, page))
                        # await asyncio.gather(asyncio.ensure_future(self.save(res_data, token, page)))
                    except Exception as why:
                        self.logger.error(f'error while requesting page {page} cause of {why}')

    @staticmethod
    def deal_products(token, rows):
        for row in rows:
            for item in row['sku_list']:
                index = item['goods_sku'].find('@#')
                new_sku = item['goods_sku'][0:index] if(index >= 0) else item['goods_sku']
                yield (row['parent_sku'], item['goods_sku'],  new_sku,
                row['product_id'], token['suffix'], token['selleruserid'], item['storage'], str(datetime.datetime.today())[:19])

    def save(self, rows, token, page):
        sql = 'insert into ibay365_vova_list (code,sku,newsku,itemid,suffix,selleruserid,storage,updateTime) values (%s,%s,%s,%s,%s,%s,%s,%s)'
        self.cur.executemany(sql, rows)
        self.con.commit()
        self.logger.info(f"success to save data page {page} in async way of suffix {token['suffix']} ")

    def run(self):
        try:
            self.clean()
            loop = asyncio.get_event_loop()
            executor = ThreadPoolExecutor(3)

            tokens = self.get_vova_token()
            tasks = []
            for token in tokens:
                # task = asyncio.ensure_future(self.get_products(token))
                # task.add_done_callback()
                # tasks.append(loop.run_in_executor(executor, self.get_products, token))
                tasks.append(asyncio.ensure_future(self.get_products(token)))
            loop.run_until_complete(asyncio.wait(tasks))
            loop.close()

        except Exception as why:
            self.logger.error(f'failed to put vova-get-product-tasks because of {why}')
        finally:
            self.close()


if __name__ == '__main__':

    import time
    start = time.time()
    worker = Uploading()
    worker.run()
    end = time.time()
    date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
    print(date + f' it takes {end - start} seconds')

