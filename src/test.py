#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-20 15:56
# Author: turpure

from src.services.base_service import CommonService
from ebaysdk.trading import Connection as Trading
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
        self.config = Config().get_config('ebay.yaml')
        self.mongo = MongoClient('192.168.0.150', 27017)
        self.mongodb = self.mongo['operation']
        self.col = self.mongodb['ebay_description_template']
        self.col1 = self.mongodb['ebay_description_group']
        # self.base_name = 'mysql'
        # self.cur = self.base_dao.get_cur(self.base_name)
        # self.con = self.base_dao.get_connection(self.base_name)

    # def close(self):
    #     self.base_dao.close_cur(self.cur)

    def get_ebay_description(self):
        try:
            api = Trading(config_file=self.config)
            trade_response = api.execute(
                'GetDescriptionTemplates',
                {
                    'CategoryID': 155350
                #     'SKU': row['Item']['SKU'],
                #     # 'SKU': '7C2796@#01',
                #     'requesterCredentials': row['requesterCredentials'],
                }
            )
            ret = trade_response.dict()
            print(ret)
            if ret['Ack'] == 'Success':
                return ret
            else:
                return []
        except Exception as e:
            self.logger.error(f"error cause of {e}")

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
            res = self.get_ebay_description()
            print(res)
            # for item in res['DescriptionTemplate']:
            #     self.col.insert_one(item)
            # for item in res['ThemeGroup']:
            #     self.col1.insert_one(item)
        except Exception as e:
            self.logger(e)
        # finally:
        #     self.close()


if __name__ == '__main__':
    sync = AliSync()
    sync.run()
