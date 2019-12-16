#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-11-05 11:02
# Author: turpure

from src.services.base_service import BaseService
from pymongo import MongoClient
import requests


class Worker(BaseService):

    def __init__(self):
        super().__init__()
        self.mongo = MongoClient(host="192.168.0.150", port=27017)
        self.mdb = self.mongo.product_engine

    @staticmethod
    def get_main_cate():
        url = f'http://www.haiyingshuju.com/wish/data/category.json'
        res = requests.get(url)
        ret = res.json()
        return ret


    @staticmethod
    def parse_sub_cate(main_cat, sub_cats):
        cats = []
        for row in sub_cats:
            cats.append(row['cname'])
        return {'cate': main_cat, 'subCate': cats}

    def save(self, row):
        col = self.mdb["wish_category"]
        col.insert_one(row)

    def run(self):
        try:
            cats = self.get_main_cate()
            for ct in cats:
                row = self.parse_sub_cate(ct['cname1'], ct['sub1'])
                self.save(row)
        except Exception as why:
            self.logger.error(f"fail to get wish cate cause of {why}")
        finally:
            self.close()
            self.mongo.close()


if __name__ == '__main__':
    worker = Worker()
    worker.run()


