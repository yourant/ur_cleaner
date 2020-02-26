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
    def get_main_cate(country):
        url = f'http://www.haiyingshuju.com/category/shopee/{country}/data/category.json'
        res = requests.get(url)
        ret = res.json()
        return ret

    @staticmethod
    def get_sub_cate(cate_id, country):
        url = f'http://www.haiyingshuju.com/category/shopee/{country}/data/{cate_id}/category.json'
        res = requests.get(url)
        ret = res.json()
        return ret

    @staticmethod
    def parse_sub_cate(countryId, country, main_cat, sub_cats):
        cats = []
        for row in sub_cats:
            cats.append(row['cname'])
        return {'plat': 'shopee', 'countryId': countryId, 'country': country, 'cate': main_cat, 'subCate': cats}

    def save(self, row):
        col = self.mdb["shopee_category"]
        col.insert(row)

    def run(self):
        try:
            countries = {'1': 'Malaysia', '2': 'Indonesia', '3': 'Thailand', '4': 'Philippines', '5': 'Taiwan', '6': 'Singapore', '7': 'Vietnam'}
            for mk, site in countries.items():
                cats = self.get_main_cate(site)
                for ct in cats:
                    sub_cats = self.get_sub_cate(ct['cid'], site)
                    row = self.parse_sub_cate(mk, site, ct['cname'], sub_cats)
                    self.save(row)
        except Exception as why:
            self.logger.error(f"fail to get shopee cate cause of {why}")
        finally:
            self.close()
            self.mongo.close()


if __name__ == '__main__':
    worker = Worker()
    worker.run()


