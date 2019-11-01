#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-10-19 14:23
# Author: turpure


import requests
import json
import datetime
import math
from pymongo import MongoClient
import motor.motor_asyncio
from src.services.base_service import BaseService
from configs.config import Config
import asyncio
import aiohttp


class Worker(BaseService):

    def __init__(self, rule_id=None):
        super().__init__()
        self.rule_id = rule_id
        config = Config()
        self.haiying_info = config.get_config('haiying')
        self.mongo = MongoClient('192.168.0.150', 27017)
        self.mongo = motor.motor_asyncio.AsyncIOMotorClient('192.168.0.150', 27017)
        self.rule = self.get_rule()

    def get_rule(self):
        if self.rule_id:
            sql = (f'select id, listedTime,marketplace from proEngine.recommend_ebayNewProductRule '
                   f'where isUsed=1 and id={self.rule_id}')
        else:
            sql = 'select id, listedTime,marketplace from proEngine.recommend_ebayNewProductRule where isUsed=1 limit 1'
        self.warehouse_cur.execute(sql)
        ret = self.warehouse_cur.fetchone()
        return ret

    async def log_in(self, session):
        base_url = 'http://www.haiyingshuju.com/auth/login'
        form_data = {
            'username': self.haiying_info['username'],
            'password': self.haiying_info['password']
        }
        ret = await session.post(base_url, data=form_data)
        return ret.headers['token']

    async def get_product(self):
        url = "http://www.haiyingshuju.com/ebay/newProduct/list"
        rule = self.rule
        async with aiohttp.ClientSession() as session:
            token = await self.log_in(session)
            rule_id = 'ebay_new_rule' + '-' + str(rule['id'])
            time_range = rule['listedTime'].split(',')
            marketplace = getattr(rule, 'marketplace', [])
            if marketplace:
                marketplace = marketplace.split(',')
            payload = {
                "cids":"","index":1,"title":"","itemId":"","soldEnd":"","country":1,"visitEnd":"","priceEnd":"",
                "soldStart":"","titleType":"","sort":"DESC","pageSize":20,"priceStart":"","visitStart":"",
                "marketplace": marketplace,"popularStatus":"","sellerOrStore":"","storeLocation":["中国"],
                "salesThreeDayFlag":"","orderColumn":"last_modi_time",
                "listedTime":[self._get_date_some_days_ago(i) for i in time_range],"itemLocation":[]}

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

            response = await session.post(url, data=json.dumps(payload), headers=headers)
            ret = await response.json()
            total = ret['total']
            total_page = math.ceil(total / 20)
            rows = self._mark_rule_id(ret['data'], rule_id)
            await self.save(rows, page=1)
            if total_page > 1:
                for page in range(2, total_page + 1):
                    payload['index'] = page
                    try:
                        response = await session.post(url, data=json.dumps(payload), headers=headers)
                        res = await response.json()
                        rows = self._mark_rule_id(res['data'], rule_id)
                        await self.save(rows, page)
                    except Exception as why:
                        self.logger.error(f'error while requesting page {page} cause of {why}')

    @staticmethod
    def _get_date_some_days_ago(number):
        today = datetime.datetime.today()
        ret = today - datetime.timedelta(days=int(number))
        return str(ret)[:10]

    @staticmethod
    def _mark_rule_id(rows, rule_id):
        for row in rows:
            row['ruleId'] = rule_id
        return rows

    async def save(self, rows, page):
        db = self.mongo["product_engine"]
        collection = db["ebay_new_product"]
        for row in rows:
            try:
                await collection.insert(row)
                self.logger.debug(f'success to save {row["itemId"]}')
            except Exception as why:
                self.logger.debug(f'fail to save {row["itemId"]} cause fo {why}')
        self.logger.info(f'success to save page {page} in async way ')

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
    worker = Worker(rule_id=4)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(worker.run())
    end = time.time()
    print(f'it takes {end - start} seconds')

