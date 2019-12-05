#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-10-19 14:23
# Author: turpure


import asyncio
import datetime
import json
import math

import aiohttp
from bson.objectid import ObjectId
from sync.haiying_spider.spider import BaseSpider
from pymongo.errors import DuplicateKeyError


class Worker(BaseSpider):

    def __init__(self, rule_id=None):
        super().__init__()

    async def get_rule(self):
        col = self.mongodb['ebay_hot_rule']
        if self.rule_id:
            rule = await col.find_one(ObjectId(self.rule_id))
            rules = [rule]
        else:
            rules = await col.find().to_list(length=None)
        return await self.parse_rule(rules)

    async def get_product(self, rule):
        url = "http://www.haiyingshuju.com/ebay/product/list"
        async with aiohttp.ClientSession() as session:
            token = await self.log_in(session)
            self.headers['token'] = token
            rule_id = rule['_id']
            del rule['_id']
            gen_end = self._get_date_some_days_ago(rule.get('genTimeStart', ''))
            gen_start = self._get_date_some_days_ago(rule.get('genTimeEnd', ''))
            rule['genTimeEnd'] = gen_end
            rule['genTimeStart'] = gen_start

            response = await session.post(url, data=json.dumps(rule), headers=self.headers)
            ret = await response.json()
            total_page = math.ceil(ret['total'] / 20)
            rows = ret['data']
            await self.save(rows, page=1, rule_id=rule_id)
            if total_page > 1:
                for page in range(2, total_page + 1):
                    try:
                        rule['index'] = page
                        response = await session.post(url, data=json.dumps(rule), headers=self.headers)
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
        today = str(datetime.datetime.now())
        for row in rows:
            row['ruleType'] = "ebay_hot_rule",
            row["rules"] = [rule_id]
            row['recommendDate'] = today
            row['recommendToPersons'] = []
            try:
                await collection.insert_one(row)
                self.logger.debug(f'success to save {row["itemId"]}')
            except DuplicateKeyError:
                doc = await  collection.find_one({'itemId': row['itemId']})
                rules = list(set(doc['rules'] + row['rules']))
                row['rules'] = rules
                row['recommendDate'] = today
                del row['recommendToPersons']
                del row['_id']
                await collection.find_one_and_update({'itemId': row['itemId']}, {"$set": row})
                self.logger.debug(f'update {row["itemId"]}')
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
