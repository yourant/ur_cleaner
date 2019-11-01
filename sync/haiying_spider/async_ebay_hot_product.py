#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-10-19 14:23
# Author: turpure


import requests
import json
import math
import aiohttp
import asyncio
import motor.motor_asyncio
from bson.objectid import ObjectId
from src.services.base_service import BaseService
from configs.config import Config


class Worker(BaseService):

    def __init__(self, rule_id=None):
        super().__init__()
        self.rule_id = rule_id
        config = Config()
        self.haiying_info = config.get_config('haiying')
        self.mongo = motor.motor_asyncio.AsyncIOMotorClient('192.168.0.150', 27017)
        self.mongodb = self.mongo['product_engine']

    async def get_rule(self):
        col = self.mongodb['ebay_hot_rule']
        if self.rule_id:
            rule = await col.find_one(ObjectId(self.rule_id))
        else:
            rule = await col.find_one()
        return rule

    async def log_in(self, session):
        base_url = 'http://www.haiyingshuju.com/auth/login'
        form_data = {
            'username': self.haiying_info['username'],
            'password': self.haiying_info['password']
        }
        ret = await session.post(base_url, data=form_data)
        return ret.headers['token']

    async def get_product(self):
        url = "http://www.haiyingshuju.com/ebay/product/list"
        async with aiohttp.ClientSession() as session:
            token = await self.log_in(session)
            rule = await self.get_rule()
            rule_id = 'ebay_new_rule' + '-' + str(rule['_id'])
            del rule['_id']
            headers = {
                'Accept': "application/json, text/plain, */*",
                'Accept-Encoding': "gzip, deflate",
                'Accept-Language': "zh-CN,zh;q=0.9,en;q=0.8",
                'Connection': "keep-alive",
                'Content-Type': "application/json",
                'Host': "www.haiyingshuju.com",
                'Origin': "http://www.haiyingshuju.com",
                'Referer': "http://www.haiyingshuju.com/ebay/index.html",
                'token': token,
                'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36",
                'Cache-Control': "no-cache",
                'cache-control': "no-cache"
            }
            response = await session.post(url, data=json.dumps(rule), headers=headers)
            ret = await response.json()
            total_page = math.ceil(ret['total'] / 20)
            rows = self._mark_rule_id(ret['data'], rule_id)
            await self.save(rows, page=1)
            if total_page > 1:
                for page in range(2, total_page + 1):
                    try:
                            rule['index'] = page
                            response = await session.post(url, data=json.dumps(rule), headers=headers)
                            res = await response.json()
                            rows = self._mark_rule_id(res['data'], rule_id)
                            await self.save(rows, page)

                    except Exception as why:
                        self.logger.error(f'fail to get page {page} cause of {why}')



    @staticmethod
    def _mark_rule_id(rows, rule_id):
        for row in rows:
            row['ruleId'] = rule_id
        return rows

    async def save(self, rows, page):
        collection = self.mongodb["ebay_hot_product"]
        for row in rows:
            try:
                await collection.insert(row)
                self.logger.debug(f'success to save {row["itemId"]}')
            except Exception as why:
                self.logger.debug(f'fail to save {row["itemId"]} cause of {why}')
        self.logger.info(f'success to get ebay page {page} hot products in async way ')

    async def run(self):
        try:
            await self.get_product()
        except Exception as why:
            self.logger.error(f'fail to get ebay products cause of {why} in async way')
        finally:
            self.close()
            self.mongo.close()


if __name__ == '__main__':
    import time
    start = time.time()
    worker = Worker(rule_id='5dafe65638fb930f1c7dbeca')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(worker.run())
    end = time.time()
    print(f'it takes {end - start} seconds')
