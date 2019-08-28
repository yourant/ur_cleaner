#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-08-28 13:57
# Author: turpure

import pymongo
from src.services.base_service import BaseService

MONGO_HOST = '127.0.0.1'
MONGO_PORT = 27017
MONGO_DB = 'crawlab_test'

mongo = pymongo.MongoClient(host=MONGO_HOST, port=MONGO_PORT)
db = mongo[MONGO_DB]
col = db['sku_image']


class PutMongo(BaseService):

    def get_data(self):

        sql = 'SELECT sku, bmpFileName as image , 0 as http_status from B_goodsSku '
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    @staticmethod
    def put_mongo(rows):
        col.insert_many(rows)

    def run(self):
        import time
        start = time.time()
        try:
            images = self.get_data()
            self.put_mongo(images)
        except Exception as e:
            self.logger.error('fail to get sku image because of {}'.format(e))
        finally:
            self.close()
            mongo.close()
            self.logger.info('it takes {} seconds'.format(time.time() - start))


if __name__ == '__main__':
    worker = PutMongo()
    worker.run()



