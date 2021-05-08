#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure


import os
import time
import datetime
from src.services.base_service import CommonService
from multiprocessing.pool import ThreadPool as Pool
from ebaysdk.trading import Connection as Trading
from ebaysdk import exception
from configs.config import Config
import json


class Sync(CommonService):
    """
    sync
    """
    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')
        self.task = self.get_mongo_collection('operation', 'ebay_off_shelf_task')

    def ending_item(self, row):
        # print(row)
        token = row['accessToken']
        item_id = row['item_id']
        api = Trading(config_file=self.config)
        # if True:
        try:
            for i in range(2):
                try:
                    response = api.execute(
                        'EndFixedPriceItem',
                        {
                            'ItemID': item_id,
                            'EndingReason': 'NotAvailable',
                            'requesterCredentials': {'eBayAuthToken': token},
                        }
                    )
                    res = response.json()
                    ret = json.loads(res)
                    print(ret)
                    if ret["Ack"] == 'Success' or ret["Ack"] == 'Warning':
                        row['status'] = 'success'
                        row['executedResult'] = 'success'
                        row['executedTime'] = datetime.datetime.today()
                        self.task.update_one({'_id': row['_id']}, {"$set": row}, upsert=True)
                        self.logger.info(f'success {row["suffix"]} to update {row["item_id"]}')
                        # self.logger.info(f'success')
                        break
                    else:
                        row['status'] = 'failed'
                        row['executedResult'] = ret["Errors"]["ShortMessage"]
                        row['executedTime'] = datetime.datetime.today()
                        self.task.update_one({'_id': row['_id']}, {"$set": row}, upsert=True)
                        self.logger.error(
                            f'fail to ending item {item_id} cause of  {ret["Errors"]["ShortMessage"]} and trying {i} times')
                except exception.ConnectionError as e:
                    # error = e.response.json()
                    row['status'] = 'failed'
                    row['executedResult'] = e.response.json()
                    row['executedTime'] = datetime.datetime.today()
                    self.task.update_one({'_id': row['_id']}, {"$set": row}, upsert=True)
                    self.logger.error('Item {} connect to failed cause of {}'.format(item_id, e))
        except Exception as e:
            self.logger.error('Item {} end failed cause of {}'.format(item_id, e))

    def work(self):
        try:

            tokens = self.task.find({'status': '初始化'})
            # tokens = self.task.find({'item_id': '114758183245'})
            pl = Pool(16)
            pl.map(self.ending_item, tokens)
        except Exception as why:
            self.logger.error(why)
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == "__main__":
    start = time.time()
    worker = Sync()
    worker.work()

    end = time.time()
    date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
    print(date + f' it takes {end - start} seconds')
