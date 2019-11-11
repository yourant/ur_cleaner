#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-10-19 14:23
# Author: turpure


import asyncio
import datetime
import json
import math
import copy

import aiohttp
from bson.objectid import ObjectId
from sync.haiying_spider.spider import BaseSpider
from pymongo.errors import DuplicateKeyError


class Worker(BaseSpider):

    def __init__(self, rule_id=None):
        super().__init__()

    async def get_rule(self):
        col = self.mongodb['ebay_new_rule']
        if self.rule_id:
            rule = await col.find_one(ObjectId(self.rule_id))
            rules = [rule]
        else:
            rules = await col.find().to_list(length=None)
        return await self.parse_rule(rules)

    @staticmethod
    async def parse_rule(rules):
        ret = []
        for rl in rules:
            published_site = rl['site']
            for site in published_site:
                row = copy.deepcopy(rl)
                # row['marketplace'] = [list(site.keys())[0]]
                row['marketplace'] = []
                row['country'] = list(site.values())[0]
                ret.append(row)
        return ret

    async def get_product(self, rule):
        url = "http://www.haiyingshuju.com/ebay/newProduct/list"
        async with aiohttp.ClientSession() as session:
            token = await self.log_in(session)
            self.headers['token'] = token
            try:
                rule_id = rule['_id']
                del rule['_id']
            except Exception as why:
                print(why)
            time_range = rule['listedTime']
            rule['listedTime'] = [self._get_date_some_days_ago(i) for i in time_range]
            payload = rule

            response = await session.post(url, data=json.dumps(payload), headers=self.headers)
            ret = await response.json()
            total = ret['total']
            total_page = math.ceil(total / 20)
            rows = ret['data']
            await self.save(rows, page=1, rule_id=rule_id)
            if total_page > 1:
                for page in range(2, total_page + 1):
                    payload['index'] = page
                    try:
                        response = await session.post(url, data=json.dumps(payload), headers=self.headers)
                        res = await response.json()
                        await self.save(res['data'], page, rule_id)
                    except Exception as why:
                        self.logger.error(f'error while requesting page {page} cause of {why}')

    async def save(self, rows, page, rule_id):
        collection = self.mongodb.ebay_new_product
        today = str(datetime.datetime.now())
        for row in rows:
            try:
                row['ruleType'] = "ebay_new_rule",
                row["rules"] = [rule_id]
                row['recommendDate'] = today
                await collection.insert_one(row)
                self.logger.debug(f'success to save {row["itemId"]}')
            except DuplicateKeyError:
                doc = await  collection.find_one({'itemId': row['itemId']})
                rules = list(set(doc['rules'] + row['rules']))
                await collection.find_one_and_update({'itemId': row['itemId']}, {"$set": {"rules": rules}})
            except Exception as why:
                self.logger.debug(f'fail to save {row["itemId"]} cause of {why}')
        self.logger.info(f'success to save page {page} in async way of rule {rule_id} ')


if __name__ == '__main__':
    import time
    start = time.time()
    worker = Worker()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(worker.run())
    end = time.time()
    print(f'it takes {end - start} seconds')

