#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-11-06 13:19
# Author: turpure

from src.services.base_service import BaseService
from pymongo import MongoClient, DESCENDING
import random
import datetime


class Worker(BaseService):

    def __init__(self):
        super().__init__()
        self.mongo = MongoClient(host="192.168.0.150", port=27017)
        self.mdb = self.mongo.product_engine

    @staticmethod
    def get_dispatch_rule():
        person = [
            "宋现中", "王漫漫", "陈微微", "刘珊珊", "常金彩", "廖露露", "admin", "李星", "柴盼盼", "杨笑天", "史新慈",
            "詹莹莹", "胡小红", "尚显贝", "辜星燕", "毕郑强", "王雪姣", "胡宁", "王丽6", "张崇", "徐胜东", "张杜娟",
            "张小辉", "刘霄敏", "杨晶媛", "邹雅丽", "刘爽", "潘梦晗"]

        # return random.sample(person, 2)
        return ['陈微微', '王慢慢']

    def get_to_dispatch_product(self, product_type):
        col = self.mdb[f'ebay_{product_type}_product']
        cur = col.find().sort([('sold', DESCENDING)]).limit(100)
        for row in cur:
            row['productType'] = product_type
            yield row

    def dispatch(self, rule, products):
        for pt in products:
            pt['receiver'] = rule
            pt['dispatchDate'] = str(datetime.datetime.now())
            self.save(pt)

    def save(self, row):
        col = self.mdb['ebay_recommended_product']
        try:
            col.insert_one(row)
        except Exception as why:
            self.logger.debug(f'fail to save row cause of {why}')

    def run(self):
        try:
            product_types = ['new', 'hot']
            for tp in product_types:
                rule = self.get_dispatch_rule()
                products = self.get_to_dispatch_product(tp)
                self.dispatch(rule, products)
        except Exception as why:
            self.logger.error(f'fail to dispatch ebay recommended products cause of {why}')
        finally:
            self.close()
            self.mongo.close()


if __name__ == '__main__':
    worker = Worker()
    worker.run()
