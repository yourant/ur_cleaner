#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-12-06 10:11
# Author: turpure

import requests
from bs4 import BeautifulSoup
import datetime
from src.services.base_service import CommonService


class Worker(CommonService):

    def __init__(self):
        super().__init__()
        self.col = self.get_mongo_collection('product_engine', 'ebay_recommended_product')

    def get_url(self):
        today = str(datetime.datetime.now())[:10]
        ret = self.col.find({'recommendDate': {'$regex': today}})
        for row in ret:
            ele = dict()
            ele['itemId'] = row['itemId']
            url = 'https://www.ebay.com/itm/' + row['itemId']
            ele['url'] = url
            yield ele

    def save_one(self, row):
        try:
            self.col.find_one_and_update({'itemId': row['itemId']}, {'$set': {'images': row['images']}}, upsert=True)
        except Exception as why:
            print(f'fail to save {row["itemId"]} because of {why} ')

    @staticmethod
    def get_image(url):
        for i in range(2):
            try:
                res = requests.get(url['url'])
                html = BeautifulSoup(res.text, features='lxml')
                image_div = html.find('div', {'id': 'vi_main_img_fs'})
                image_tables = image_div.find_all('table', {'class': 'img'})
                images = []
                out = dict()
                for tb in image_tables:
                    img = tb.find('img')
                    src = img.attrs['src']
                    ret = src.replace('l64', 'l500')
                    images.append(ret)

                out['images'] = images
                out['itemId'] = url['itemId']
                return out

            except Exception as why:
                print(f'{url["url"]} fail to get image because of {why}')
        return {'images': [], 'itemId': url['itemId']}

    def trans(self):
        urls = self.get_url()
        for ul in urls:
            ret = self.get_image(ul)
            self.save_one(ret)
            print(f'success to get images of {ret["itemId"]}')

    def run(self):
        try:
            self.trans()
        except Exception as why:
            print(f'fail to get ebay images because of {why}')
        finally:
            self.mongo.close()


if __name__ == "__main__":
    worker = Worker()
    worker.run()




