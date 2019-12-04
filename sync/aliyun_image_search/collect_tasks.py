#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-11-29 10:06
# Author: turpure


from pymongo import MongoClient
from src.services.base_service import BaseService
import datetime


class Worker(BaseService):

    def __init__(self):
        super().__init__()
        self.mongo = MongoClient('192.168.0.150', 27017)
        self.mongodb = self.mongo['product_engine']
        self.col = self.mongodb['images_tasks']

    def get_image(self, begin_date, end_date):
        sql = ("select  bgs.BmpFileName as img ,bgs.sku, bg.goodsCode, bg.createDate from  b_goodsSku as bgs"
               " LEFT JOIN b_goods as bg on bgs.goodsid=bg.nid where bgs.bmpFileName like 'http%'  "
               "and isnull(bgs.BmpFileName, '') != '' and convert(varchar(10), createDate,121) between %s and  %s")
        self.cur.execute(sql, (begin_date, end_date))
        ret = self.cur.fetchall()
        for row in ret:
            row['doneFlag'] = 0
            yield row

    def save_many(self, rows):
        self.col.insert_many(rows)

    def save_one(self, row):
        try:
            self.col.update_one({"_id": row.get('_id', '')}, row, upsert=True)
        except Exception as why:
            self.logger.debug(f'fail to save {row["sku"]} cause of {why}')

    def run(self):
        try:
            today = str(datetime.datetime.today())[:10]
            some_days_ago = str(datetime.datetime.today() - datetime.timedelta(days=60))[:10]
            images = self.get_image(some_days_ago, today)
            for ele in images:
                self.save_one(ele)
            self.logger.info('success to collect tasks of image')

        except Exception as why:
            self.logger.error(f'fail to collect tasks of image cause of {why}')

        finally:
            self.mongo.close()
            self.close()


if __name__ == '__main__':
    worker = Worker()
    worker.run()







