#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-11-08 17:02
# Author: turpure


import datetime
from abc import ABCMeta, abstractmethod
from pymongo import MongoClient
import motor.motor_asyncio
import copy
from src.services.base_service import BaseService
from configs.config import Config
from sync.haiying_spider.config import headers


class BaseSpider(BaseService):

    def __init__(self, rule_id=None):
        super().__init__()
        self.rule_id = rule_id
        self.headers = headers
        config = Config()
        self.haiying_info = config.get_config('haiying')
        self.mongo = MongoClient('192.168.0.150', 27017)
        self.mongo = motor.motor_asyncio.AsyncIOMotorClient('192.168.0.150', 27017)
        self.mongodb = self.mongo['product_engine']

    @abstractmethod
    async def get_rule(self):
        pass

    async def log_in(self, session):
        base_url = 'http://www.haiyingshuju.com/auth/login'
        form_data = {
            'username': self.haiying_info['username'],
            'password': self.haiying_info['password']
        }
        ret = await session.post(base_url, data=form_data)
        return ret.headers['token']



    @abstractmethod
    async def get_product(self, rule):
        pass

    @staticmethod
    def _get_date_some_days_ago(number):
        today = datetime.datetime.today()
        ret = today - datetime.timedelta(days=int(number))
        return str(ret)[:10]

    @abstractmethod
    async def save(self, session, rows, page, rule):
        pass

    async def run(self):
        try:
            rules = await self.get_rule()
            for rls in rules:
                await self.get_product(rls)
        except Exception as why:
            self.logger.error(f'fail to get wish products cause of {why} in async way')
        finally:
            self.close()
            self.mongo.close()






