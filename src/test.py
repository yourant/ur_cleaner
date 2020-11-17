#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-20 15:56
# Author: turpure

from src.services.base_service import CommonService
import datetime
from pymongo import MongoClient
from configs.config import Config


# class AliSync(BaseService):
class AliSync(CommonService):
    """
    check purchased orders
    """

    def __init__(self):
        super().__init__()
        self.mongo = MongoClient('192.168.0.150', 27017)
        self.mongodb = self.mongo['operation']
        self.col = self.mongodb['ebay_site']
        self.base_name = 'mysql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)



    def get_ebay_shipping(self):
        # sql = 'SELECT servicesName,type,site,ibayShipping FROM proCenter.oa_shippingService '
        sql = 'SELECT name,nameEn as name_en,code,currencyCode as currency_code FROM proCenter.oa_siteCountry '
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        print(ret)
        for row in ret:
            print(row)
            self.col.insert_one(row)
            # yield row

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
            res = self.get_ebay_shipping()
            print(res)
        except Exception as e:
            self.logger(e)
        finally:
            self.close()


if __name__ == '__main__':
    sync = AliSync()
    sync.run()
