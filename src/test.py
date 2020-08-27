#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-20 15:56
# Author: turpure

from src.services.base_service import BaseService
import datetime
from pymongo import MongoClient

mongo = MongoClient('192.168.0.172', 27017)
mongodb = mongo['operation']
col = mongodb['ebay_fee']


class AliSync(BaseService):
    """
    check purchased orders
    """

    def __init__(self):
        super().__init__()

    @staticmethod
    def get_data(begin, end):
        rows = col.find({'Date': {'$gte': begin, '$lte': end}})
        for row in rows:
            yield (row['accountName'], row['feeType'], row['value'], row['currency'],
                   row['Date'], str(row['Date'])[:10], row['description'], row['itemId'], row['memo'],
                   row['transactionId'], row['orderId'], row['recordId'])

    def insert(self, row):
        sql = (
            'insert into y_fee(notename,fee_type,total,currency_code,fee_time,batchId,description,itemId,memo,'
            'transactionId,orderId,recordId) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '
        )
        try:
            self.cur.executemany(sql, row)
            self.con.commit()
        except Exception as e:
            self.logger.error("%s while trying to save data" % e)

    def clean(self, begin, end):
        sql = "delete y_fee where fee_time between %s and %s"
        self.cur.executemany(sql, (begin, end))
        self.con.commit()
        self.logger.info(f'success to clean y_fee fee time between {begin} and {end}')

    def run(self):
        try:
            begin = '2020-08-20'
            end = str(datetime.datetime.now())[:10]
            rows = self.get_data(begin, end)
            for row in rows:
                print(row)
            # self.insert(rows)
        except Exception as e:
            self.logger(e)
        finally:
            self.close()


if __name__ == '__main__':
    sync = AliSync()
    sync.run()
