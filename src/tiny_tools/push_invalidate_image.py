#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-08-29 16:56
# Author: turpure

import pymongo
from src.services.base_service import BaseService
MONGO_HOST = '127.0.0.1'
MONGO_PORT = 27017
MONGO_DB = 'crawlab_test'

mongo = pymongo.MongoClient(host=MONGO_HOST, port=MONGO_PORT)
db = mongo[MONGO_DB]
col = db['sku_image']


class ImagePusher(object):

    @staticmethod
    def get_invalidate_image():
        images = col.find({'http_status': {'$ne': 200}}).limit(100)
        for row in images:
            print(row)

    def run(self):
        try:
            self.get_invalidate_image()
        except Exception as why:
            print(f'failed cause of {why}')


if __name__ == '__main__':
    worker = ImagePusher()
    worker.run()
