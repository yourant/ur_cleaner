#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-10-19 14:23
# Author: turpure


import json
import math
import aiohttp
import asyncio
import datetime
import motor.motor_asyncio
from bson.objectid import ObjectId
from src.services.base_service import BaseService
from configs.config import Config
from pymongo.errors import DuplicateKeyError


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
            rule = [rule]
        else:
            rule = await col.find().to_list(length=None)
        return rule

    async def log_in(self, session):
        base_url = 'http://www.haiyingshuju.com/auth/login'
        form_data = {
            'username': self.haiying_info['username'],
            'password': self.haiying_info['password']
        }
        ret = await session.post(base_url, data=form_data)
        return ret.headers['token']

    async def get_product(self, rule):
        url = "http://www.haiyingshuju.com/ebay/product/list"
        async with aiohttp.ClientSession() as session:
            token = await self.log_in(session)
            rule_id = rule['_id']
            del rule['_id']
            gen_end = self._get_date_some_days_ago(rule.get('genTimeStart', ''))
            gen_start = self._get_date_some_days_ago(rule.get('genTimeEnd', ''))
            rule['genTimeEnd'] = gen_end
            rule['genTimeStart'] = gen_start

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
            rows = ret['data']
            await self.save(rows, page=1, rule_id=rule_id)
            if total_page > 1:
                for page in range(2, total_page + 1):
                    try:
                        rule['index'] = page
                        response = await session.post(url, data=json.dumps(rule), headers=headers)
                        res = await response.json()
                        rows = res['data']
                        await self.save(rows, page, rule_id)

                    except Exception as why:
                        self.logger.error(f'fail to get page {page} cause of {why}')

    @staticmethod
    def _get_date_some_days_ago(number):
        if number:
            today = datetime.datetime.today()
            ret = today - datetime.timedelta(days=int(number))
            return str(ret)[:10]
        return number

    async def save(self, rows, page, rule_id):
        collection = self.mongodb.ebay_hot_product
        for row in rows:
            try:
                row['ruleType'] = "ebay_hot_rule",
                row["rules"] = [rule_id]
                await collection.insert_one(row)
                self.logger.debug(f'success to save {row["itemId"]}')
            except DuplicateKeyError:
                doc = await  collection.find_one({'itemId': row['itemId']})
                rules = list(set(doc['rules'] + row['rules']))
                await collection.find_one_and_update({'itemId': row['itemId']}, {"$set": {"rules": rules}})
            except Exception as why:
                self.logger.debug(f'fail to save {row["itemId"]} cause of {why}')
        self.logger.info(f'success to save page {page} in async way of rule {rule_id} ')

    async def run(self):
        try:
            rules = await self.get_rule()
            for rus in rules:
                await self.get_product(rus)
        except Exception as why:
            self.logger.error(f'fail to get ebay products cause of {why} in async way')
        finally:
            self.close()
            self.mongo.close()


if __name__ == '__main__':
    import time
    start = time.time()
    worker = Worker()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(worker.run())
    end = time.time()
    print(f'it takes {end - start} seconds')
