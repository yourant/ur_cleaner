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
            rules = await col.find({'isUsed': 1, 'type': 'auto'}).to_list(length=None)
        return await self.parse_rule(rules)

    async def get_product(self, rule):
        url = "http://www.haiyingshuju.com/ebay/newProduct/list"
        async with aiohttp.ClientSession() as session:
            token = await self.log_in(session)
            self.headers['token'] = token
            rule_id = rule['_id']
            ruleData = {'id': rule['_id'], 'ruleName': rule['ruleName']}
            del rule['_id']
            time_range = rule['listedTime']
            rule['listedTime'] = [self._get_date_some_days_ago(i) for i in time_range]
            payload = rule

            response = await session.post(url, data=json.dumps(payload), headers=self.headers)
            ret = await response.json()
            total = ret['total']
            total_page = math.ceil(total / 20)
            rows = ret['data']
            await self.save(session, rows, page=1, rule=ruleData)
            if total_page > 1:
                for page in range(2, total_page + 1):
                    payload['index'] = page
                    try:
                        response = await session.post(url, data=json.dumps(payload), headers=self.headers)
                        res = await response.json()
                        await self.save(session, res['data'], page, rule=ruleData)
                    except Exception as why:
                        self.logger.error(f'error while requesting page {page} cause of {why}')

    async def save(self, session, rows, page, rule):
        collection = self.mongodb.ebay_new_product
        today = str(datetime.datetime.now())
        for row in rows:
            row['ruleType'] = "ebay_new_rule",
            row["rules"] = [rule['id']]
            row["ruleName"] = rule['ruleName']
            row['recommendDate'] = today
            row['recommendToPersons'] = []
            try:
                await collection.insert_one(row)
                self.logger.debug(f'success to save {row["itemId"]}')
            except DuplicateKeyError:
                doc = await  collection.find_one({'itemId': row['itemId']})
                rules = list(set(doc['rules'] + row['rules']))
                row['rules'] = rules
                del row['_id']
                del row['recommendToPersons']
                await collection.find_one_and_update({'itemId': row['itemId']}, {"$set": row})
                self.logger.debug(f'update {row["itemId"]}')
            except Exception as why:
                self.logger.debug(f'fail to save {row["itemId"]} cause of {why}')
        self.logger.info(f"success to save page {page} in async way of rule {rule['id']} ")


if __name__ == '__main__':
    import time
    start = time.time()
    worker = Worker()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(worker.run())
    end = time.time()
    print(f'it takes {end - start} seconds')

