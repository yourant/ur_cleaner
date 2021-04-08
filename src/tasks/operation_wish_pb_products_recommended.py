#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-09-09 13:30
# Author: turpure

import os
from src.services.base_service import CommonService


class Worker(CommonService):
    """
    worker template
    """

    def __init__(self):
        super().__init__()
        self.from_col = self.get_mongo_collection('operation', 'wish_products')
        self.to_col = self.get_mongo_collection('operation', 'wish_productboost_recommended')

    def close(self):
        self.mongo.close()

    def get_products(self):
        products = self.from_col.find().limit(100)
        for pros in products:
            pros['pb_status'] = 0
            yield pros

    def save_products(self, product):
        self.to_col.update_one({'id': product['id']}, {"$set": product}, upsert=True)
        self.logger.info(f'success to save {product["id"]}')

    def trans(self):
        products = self.get_products()
        for pd in products:
            self.save_products(pd)

    def work(self):
        try:
            self.trans()
        except Exception as why:
            self.logger.error('fail to finish task cause of {} '.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


