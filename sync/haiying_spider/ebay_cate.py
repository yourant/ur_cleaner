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
    def get_main_cate(marketplace):
        url = f'http://www.haiyingshuju.com/category/ebay/{marketplace}/data/category.json'
        res = requests.get(url)
        ret = res.json()
        return ret

    @staticmethod
    def get_sub_cate(cate_id, marketplace):
        url = f'http://www.haiyingshuju.com/category/ebay/{marketplace}/data/{cate_id}/category.json'
        res = requests.get(url)
        ret = res.json()
        return ret

    @staticmethod
    def parse_sub_cate(marketplace, main_cat, sub_cats):
        cats = []
        for row in sub_cats:
            cats.append(row['cname'])
        return {'plat': 'ebay', 'marketplace': marketplace, 'cate': main_cat, 'subCate': cats}

    def save(self, row):
        col = self.mdb["ebay_category"]
        col.insert(row)

    def run(self):
        try:
            markets = {'EBAY_US': 'us', 'EBAY_GB': 'uk', 'EBAY_DE': 'de'}
            for mk, site in markets.items():
                cats = self.get_main_cate(site)
                for ct in cats:
                    sub_cats = self.get_sub_cate(ct['cid'], site)
                    row = self.parse_sub_cate(mk, ct['cname'], sub_cats)
                    self.save(row)
        except Exception as why:
            self.logger.error(f"fail to get ebay cate cause of {why}")
        finally:
            self.close()
            self.mongo.close()


if __name__ == '__main__':
    worker = Worker()
    worker.run()


