#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-09-17 16:57
# Author: turpure

import os
from src.services.base_service import CommonService
import aiohttp
import json
import asyncio
import re


class OffShelf(CommonService):

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    def get_vova_token(self):
        # sql = ("EXEC B_VovaOffShelfProducts  '清仓,停产,停售,线上清仓,线下清仓,线上清仓50P,线上清仓100P,春节放假'," +
        #       "'爆款,旺款,Wish新款,浮动款,在售,侵权'," +
        #       "'清仓,停产,停售,线上清仓,线上清仓50P,线上清仓100P,春节放假'")

        sql = ("EXEC B_VovaOffShelfProducts  '停产,停售,春节放假'," +
               "'爆款,旺款,Wish新款,浮动款,在售,侵权'," +
               "'停产,停售,春节放假'")
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        return ret

    async def update_products_storage(self, token, sema):
        """
        1. 所有SKu都为0，就改成1
        2. 参见活动的产品，数量改为顾客指定数量
        3. 并发
        """
        url = 'https://merchant.vova.com.hk/api/v1/product/updateGoodsData'
        goods_info = {
            "product_id": token["itemid"],
            "goods_sku": token["sku"],
            "attrs": {
                "storage": int(token["quantity"])
            }
        }
        param = {
            "token": token['token'],
            "goods_info": [goods_info]
        }
        try:
            async with sema:
                async with aiohttp.ClientSession() as session:
                    response = await session.post(url, data=json.dumps(param))
                    res = await response.json(content_type='application/json')
                    if res['execute_status'] == 'success':
                        self.logger.info(f"success to off shelf vova product itemid:{token['itemid']},sku:{token['sku']}")
                    else:
                        if '存在被顾客预定' in res['message']:
                            find_number = re.findall(r'存在被顾客预定(\d)件', res['message'])
                            if find_number:
                                token['storage'] = find_number[0]
                                await self.update_products_storage(token)
                        if '标准库存不能全为0' in res['message']:
                            await self.disable_product(token,session)

                        else:
                            self.logger.error(f"failed to off shelf vova product itemid:{token['itemid']},"
                                          f"sku:{token['sku']} because of {res['message']}")
        except Exception as error:
            self.logger.error(f'fail to update products  of {token["sku"]} cause of {error}')

    async def disable_product(self, token, session):
        item = {
                    "token": token['token'],
                    "goods_list": [token['itemid']]
        }
        url = 'https://merchant.vova.com.hk/api/v1/product/disableSale'
        try:
            response = session.post(url, data=json.dumps(item))
            res = response.json(content_type='application/json')
            self.logger.info(f"{res['execute_status']} to disable product {token['itemid']}")
        except Exception as why:
            self.logger.error(f'fail to disable {token["itemid"]} casue of {why}')

    def run(self):
        try:
            sema = asyncio.Semaphore(50)
            loop = asyncio.get_event_loop()
            tokens = self.get_vova_token()
            if tokens:
                tasks = [asyncio.ensure_future(self.update_products_storage(tk, sema)) for tk in tokens]
                loop.run_until_complete(asyncio.wait(tasks))
                loop.close()
        except Exception as why:
            self.logger.error(f'failed to put vova-get-product-tasks because of {why}')
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == '__main__':

    import time
    start = time.time()
    worker = OffShelf()
    worker.run()
    end = time.time()
    date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
    print(date + f' it takes {end - start} seconds')
