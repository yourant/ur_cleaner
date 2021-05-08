#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure


import os
import time
import datetime
from src.services.base_service import CommonService
from multiprocessing.pool import ThreadPool as Pool
import requests
from configs.config import Config
import json


class Sync(CommonService):
    """
    sync
    """

    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')
        self.task = self.get_mongo_collection('operation', 'wish_off_shelf_task')
        self.product_list = self.get_mongo_collection('operation', 'wish_products')

    def remove_item(self, row):
        token = row['accessToken']
        item_id = row['item_id']
        headers = {'content-type': 'application/json', 'Authorization': 'Bearer ' + token}
        base_url = 'https://merchant.wish.com/api/v2/product/remove'
        param = {"id": item_id}
        response = requests.post(base_url, params=param, headers=headers)
        ret = response.json()
        if ret["code"] == 0:
            row['status'] = 'success'
            row['executedResult'] = 'success'
            row['executedTime'] = datetime.datetime.today()
            self.task.update_one({'_id': row['_id']}, {"$set": row}, upsert=True)
            self.product_list.delete_one({'id': item_id})
            self.logger.info(f'success {row["suffix"]} to remove {row["item_id"]}')
            return True
        else:
            row['status'] = 'failed'
            row['executedResult'] = ret['message'] if 'message' in ret else 'failed'
            row['executedTime'] = datetime.datetime.today()
            self.task.update_one({'_id': row['_id']}, {"$set": row}, upsert=True)
            self.logger.error(f"fail to remove item {item_id} cause of {row['executedResult']}")
            return False

    def ending_item(self, row):
        token = row['accessToken']
        item_id = row['item_id']
        headers = {'content-type': 'application/json', 'Authorization': 'Bearer ' + token}
        base_url = 'https://merchant.wish.com/api/v2/product/disable'
        param = {"id": item_id}
        # if True:
        try:
            for i in range(2):
                try:
                    response = requests.post(base_url, params=param, headers=headers)
                    ret = response.json()
                    # print(ret)
                    if ret["code"] == 0:
                        remove_res = self.remove_item(row)
                        if remove_res:
                            break
                    else:
                        row['status'] = 'failed'
                        row['executedResult'] = ret['message'] if 'message' in ret else 'failed'
                        row['executedTime'] = datetime.datetime.today()
                        self.task.update_one({'_id': row['_id']}, {"$set": row}, upsert=True)
                        self.logger.error(
                            f"fail to ending item {item_id} cause of {row['executedResult']} and trying {i} times")
                        # 后台已经移除的，直接删除在线listing
                        if ret["code"] == 1004:
                            self.product_list.delete_one({'id': item_id})
                            break
                except Exception as e:
                    # error = e.response.json()
                    # row['status'] = 'failed'
                    # row['executedResult'] = e.response.json()
                    # row['executedTime'] = datetime.datetime.today()
                    # self.task.update_one({'_id': row['_id']}, {"$set": row}, upsert=True)
                    self.logger.error('Item {} connect to failed cause of {}'.format(item_id, e))
        except Exception as e:
            self.logger.error('Item {} end failed cause of {}'.format(item_id, e))

    def work(self):
        try:

            # tokens = self.task.find({'status': '初始化'})
            tokens = self.task.find({'item_id': '5a9779691c6def2c4c164307'})
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
