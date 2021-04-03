#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure


import os
import re
import math
import asyncio
import time
import datetime
from src.services.base_service import CommonService
import requests
from multiprocessing.pool import ThreadPool as Pool

from pymongo import MongoClient

mongo = MongoClient('192.168.0.150', 27017)

table = mongo['operation']['wish_products']
stock = mongo['wish']['wish_sku_stock']
quantity = mongo['wish']['wish_sku_quantity']


class Sync(CommonService):
    """
    sync
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.product_stock = self.get_mongo_collection('operation', 'product_sku')
        self.product_list = self.get_mongo_collection('operation', 'wish_products')
        self.task = self.get_mongo_collection('operation', 'wish_stock_task')
        self.status = ['线下清仓']  # 改0
        self.status1 = ['爆款', '旺款', '浮动款', 'Wish新款', '在售']  # 改固定数量
        self.status2 = ['停产', '清仓', '线上清仓', '线上清仓50P', '线上清仓100P', '春节放假', '停售']  # 改实际库存'

    def close(self):
        self.base_dao.close_cur(self.cur)

    def get_wish_token(self):
        sql = ("SELECT AccessToken,aliasname FROM S_WishSyncInfo WHERE  "
               "aliasname is not null"
               " and  AliasName = 'WISE180-neatthao' "
               " and  AliasName not in "
               "(select DictionaryName from B_Dictionary where CategoryID=12 and used=1 and FitCode='Wish') ")
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_products(self, suffix):
        rows = self.product_list.find({'suffix': suffix, "removed_by_merchant": "False", "review_status": "approved",
                                       # , 'parent_sku': {'$regex': '7N0828'}
                                       # "id": "6062bc31893e9afe58745377"
                                       }, no_cursor_timeout=True)
        for rw in rows:
            for row in rw['variants']:
                new_sku = row['Variant']['sku'].split("@")[0]
                ele = {'code': row['Variant']['sku'], 'sku': row['Variant']['sku'],
                       'newsku': new_sku, 'itemid': row['Variant']['product_id'], 'suffix': rw['suffix'],
                       'selleruserid': '', 'storage': row['Variant']['inventory'],
                       'updateTime': str(datetime.datetime.today())[:19],
                       'enabled': row['Variant']['enabled'], 'removed_by_merchant': rw['removed_by_merchant']}
                ele['_id'] = ele['itemid']
                # yield (ele['code'], ele['sku'], ele['newsku'], ele['itemid'], ele['suffix'], ele['selleruserid'],
                #        ele['storage'], ele['updateTime'])
                yield {'shopSku': ele['sku'], 'sku': ele['newsku'], 'item_id': ele['itemid'],
                       'onlineInventory': ele['storage'], 'suffix': ele['suffix']}

    def get_data(self, row):
        # print(row)
        token = row['AccessToken']
        suffix = row['aliasname']
        products = self.get_products(suffix)
        for product in products:
            storage = int(product['onlineInventory'])
            sku_info = self.product_stock.aggregate([{'$match': {'SKU': {'$regex': product['sku']}, 'storeName': '义乌仓'}}])
            # if not sku_info:
            #     sku_info = stock.aggregate([{'$match': {'shopsku': {'$regex': product['newsku']}}}, {'$limit': 1}])
            for sku in sku_info:
                # print(sku)
                hope_use_num = int(sku['hopeUseNum'])
                # print(storage)
                # print(hope_use_num)
                check = self.check(storage, hope_use_num, sku['GoodsStatus'])
                print(sku['SKU'], storage, hope_use_num, sku['GoodsStatus'], check)
                # 判断sku数量是否需要修改
                # if check:
                #     inventory = self.get_quantity(storage, hope_use_num, sku['status'])
                #     # if inventory == 90000 and storage < 100 or inventory < 90000 and storage != inventory:
                #     if inventory == 90000 and storage < 100 or inventory == 0 and storage != 0:
                #         params = {'itemid': product['itemid'], 'sku': product['sku'], 'inventory': inventory,
                #                   'storage': product['storage'], 'token': token, 'suffix': suffix, 'flag': '0', }
                #         # print
                #         headers = {'content-type': 'application/json', 'Authorization': 'Bearer ' + token}
                #         base_url = 'https://merchant.wish.com/api/v2/variant/update-inventory'
                #         param = {
                #             "sku": product['sku'],
                #             "inventory": inventory
                #         }
                #         for i in range(2):
                #             try:
                #                 response = requests.get(base_url, params=param, headers=headers, timeout=20)
                #                 ret = response.json()
                #                 if ret["code"] == 0:
                #                     # 更新标记字段
                #                     table.update_one({'_id': product['itemid']}, {"$set": {'is_modify_num': 1}},
                #                                      False, True)
                #                     # self.logger.info(f'success {row["aliasname"]} to update {product["itemid"]}')
                #                     break
                #             except Exception as why:
                #                 self.logger.error(f'fail to update inventory cause of  {why} and trying {i + 1} times')
                #         # quantity.insert_one(params)

    def get_quantity(self, storage, hope_use_num, status):
        if storage <= 0:
            if status in self.status1:
                return 90000
            if status in self.status2:
                return hope_use_num if hope_use_num > 0 else 0
            if status in self.status:
                return 0
            return 0
        else:
            if status in self.status1:
                return 90000
            if status in self.status2:
                return hope_use_num if hope_use_num > 0 else 0
            if status in self.status:
                return 0
            return storage

    def check(self, storage, hope_use_num, status):
        if storage <= 0:
            if not status:
                return False
            if status not in self.status and status not in self.status1 and status not in self.status2:
                return False
            if status in self.status2 and (storage == 0 and hope_use_num == 0 or storage < hope_use_num):
                return False
            if status in self.status1 and storage == 0:
                return False
            return True
        else:
            if status in self.status1:
                if storage >= 100:
                    return False
                else:
                    return True
            if status in self.status2 and storage != hope_use_num:
                return True
            if status in self.status and storage > 0:
                return True
            return False



    def work(self):
        try:
            tokens = self.get_wish_token()

            # for token in tokens:
            #     self.get_data(token)

            pl = Pool(50)
            pl.map(self.get_data, tokens)
            pl.close()
            pl.join()

        except Exception as why:
            self.logger.error(why)
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()
            mongo.close()


if __name__ == "__main__":
    start = time.time()
    worker = Sync()
    worker.work()
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(worker.work())

    end = time.time()
    date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
    print(date + f' it takes {end - start} seconds')
