#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-09-17 16:57
# Author: turpure

from src.services.base_service import BaseService
import aiohttp
import json
import asyncio


class Uploading(BaseService):
    def __init__(self):
        super().__init__()

    async def clean(self):
        sql = 'truncate table ibay365_vova_list'
        self.cur.execute(sql)
        self.con.commit()

    async def get_vova_token(self):

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
            await self.save(rows, token, page=1)
            if total_page > 1:
                for page in range(2, total_page + 1):
                    param['conditions']['page_arr']['page'] = page
                    try:
                        response = await session.post(url, data=json.dumps(param))
                        res = await response.json()
                        res_data = self.deal_products(token, res['product_list'])
                        await self.save(res_data, token, page)
                    except Exception as why:
                        self.logger.error(f'error while requesting page {page} cause of {why}')

    @staticmethod
    def deal_products(token, rows):
        for row in rows:
            for item in row['sku_list']:
                index = item['goods_sku'].find('@#')
                new_sku = item['goods_sku'][0:index] if(index >= 0) else item['goods_sku']
                yield (row['parent_sku'], item['goods_sku'],  new_sku,
                row['product_id'], token['suffix'], token['selleruserid'], item['storage'])

    async def save(self, rows, token, page):
        sql = 'insert into ibay365_vova_list (code,sku,newsku,itemid,suffix,selleruserid,storage) values (%s,%s,%s,%s,%s,%s,%s)'
        self.cur.executemany(sql, rows)
        self.con.commit()
        self.logger.info(f"success to save data page {page} in async way of suffix {token['suffix']} ")

    async def run(self):
        try:
            await self.clean()
            tokens = await self.get_vova_token()
            for token in tokens:
                await self.get_products(token)

        except Exception as why:
            self.logger.error(f'failed to put vova-get-product-tasks because of {why}')
        finally:
            self.close()


if __name__ == '__main__':

    import time
    start = time.time()
    worker = Uploading()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(worker.run())
    end = time.time()
    print(f'it takes {end - start} seconds')

