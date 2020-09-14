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

mongo2 = MongoClient('192.168.0.150', 27017)
mongodb2 = mongo2['operation']
col2 = mongodb2['ebay_fee']


class AliSync(BaseService):
    """
    check purchased orders
    """

    def __init__(self):
        super().__init__()

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

    def get_ebay_token(self):
        sql = ("SELECT  NoteName AS suffix,EuSellerID AS storeName, MIN(EbayTOKEN) AS token "
               "FROM [dbo].[S_PalSyncInfo] WHERE SyncEbayEnable=1 "
               "and notename in (select dictionaryName from B_Dictionary "
               "where  CategoryID=12 and FitCode ='eBay' and used = 0) "
               "GROUP BY NoteName,EuSellerID ORDER BY NoteName ")
        self.cur.executemany(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    @staticmethod
    def get_data(begin):
        rows = col.find({'Date': {'$regex': begin}})
        for row in rows:
            del row['_id']
            yield row

    def get_batch_id(self):
        sql = ("select max(batchId) batchId from y_fee"
               " where notename in "
               "(select DictionaryName from B_Dictionary nolock "
               "where CategoryID=12 and FitCode ='eBay')"
               )
        try:
            self.cur.execute(sql)
            ret = self.cur.fetchone()
            batch_id = str(datetime.datetime.strptime(ret['batchId'], '%Y-%m-%d') - datetime.timedelta(days=1))[:10]
            print(batch_id)
            return batch_id
        except Exception as why:
            self.logger.error('fail to get max batchId cause of {}'.format(why))

    def run(self):
        try:
            # for i in range(33):
            #     begin = str(datetime.datetime.strptime('2020-08-01', '%Y-%m-%d') + datetime.timedelta(days=i))[:10]
            #     # print(begin)
            #     rows = self.get_data(begin)
            #     for row in rows:
            #         # print(row)
            #         col2.update_one({'recordId': row['recordId']}, {"$set": row}, upsert=True)
            #     self.logger.info(f'success to sync data in {begin}')
            self.get_batch_id()
        except Exception as e:
            self.logger(e)
        finally:
            self.close()


if __name__ == '__main__':
    sync = AliSync()
    sync.run()
