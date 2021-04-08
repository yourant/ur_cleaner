#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure


import os
import time
import datetime
from src.services.base_service import CommonService
from multiprocessing.pool import ThreadPool as Pool


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
        self.suffix_token = self.get_wish_suffix_token()
        self.status = ['线下清仓']  # 改0
        self.status1 = ['爆款', '旺款', '浮动款', 'Wish新款', '在售']  # 改固定数量
        self.status2 = ['停产', '清仓', '线上清仓', '线上清仓50P', '线上清仓100P', '春节放假', '停售']  # 改实际库存'

    def close(self):
        self.base_dao.close_cur(self.cur)

    def get_wish_suffix_token(self):
        res = dict()
        sql = ("SELECT AccessToken,aliasname FROM S_WishSyncInfo WHERE  "
               "aliasname is not null"
               " and  AliasName not in "
               "(select DictionaryName from B_Dictionary where CategoryID=12 and used=1 and FitCode='Wish') ")
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            res[row['aliasname']] = row['AccessToken']
        return res

    def get_products(self, goods_code):
    # def get_products(self):
        rows = self.product_list.find({"removed_by_merchant": "False", "review_status": "approved",
                                       'goods_code': goods_code,
                                       # 'parent_sku': {'$regex': goods_code},
                                       # 'suffix': suffix,
                                       # "id": "5f962aaeb0d3c2003d8e091d"
                                       }, no_cursor_timeout=True)
        for rw in rows:
            for row in rw['variants']:
                new_sku = row['Variant']['sku'].split("@")[0]
                new_sku = new_sku.split("*")[0]
                ele = {'code': row['Variant']['sku'], 'sku': row['Variant']['sku'],
                       'newsku': new_sku, 'itemid': row['Variant']['product_id'], 'suffix': rw['suffix'],
                       'selleruserid': '', 'storage': row['Variant']['inventory'],
                       'updateTime': str(datetime.datetime.today())[:19],
                       'enabled': row['Variant']['enabled'], 'removed_by_merchant': rw['removed_by_merchant']}
                ele['_id'] = ele['itemid']
                # yield (ele['code'], ele['sku'], ele['newsku'], ele['itemid'], ele['suffix'], ele['selleruserid'],
                #        ele['storage'], ele['updateTime'])
                yield {'shopSku': ele['sku'], 'sku': ele['newsku'], 'item_id': ele['itemid'], 'suffix': ele['suffix'],
                       'onlineInventory': ele['storage'], 'accessToken': self.suffix_token[ele['suffix']]}

    def get_data(self, row):
        goods_code = row['goodsCode']
        try:
            products = self.get_products(goods_code)
            goods_stock = self.product_stock.find({'goodscode': goods_code, 'storeName': '义乌仓'})
            goods_stock_list = list(goods_stock)
            for product in products:
                storage = int(product['onlineInventory'])
                for sku in goods_stock_list:
                    if sku['SKU'] == product['sku']:
                        hope_use_num = int(sku['hopeUseNum'])
                        check = self.check(storage, hope_use_num, sku['GoodsStatus'])
                        # print(product['item_id'], sku['SKU'], storage, hope_use_num, sku['GoodsStatus'], check)
                        # 判断sku数量是否需要修改
                        if check:
                            inventory = self.get_quantity(storage, hope_use_num, sku['GoodsStatus'])
                            # if inventory == 90000 and storage < 100 or inventory < 90000 and storage != inventory:
                            if inventory == 90000 and storage < 100 or inventory == 0 and storage != 0:
                                params = {'item_id': product['item_id'], 'suffix': product['suffix'],
                                          'sku': product['sku'], 'shopSku': product['shopSku'],
                                          'goodsCode': sku['goodscode'], 'goodsName': sku['goodsname'],
                                          'mainImage': sku['skuImageUrl'], 'goodsStatus': sku['GoodsStatus'],
                                          'onlineInventory': storage, 'targetInventory': inventory,
                                          'status': '初始化', 'accessToken': product['accessToken'],
                                          'created': str(datetime.datetime.today())[:19], 'executedResult': '',
                                          'executedTime': ''}
                                # print(params)
                                # self.task.insert_one(params)
                                self.task.update_one({'item_id': params['item_id'], 'shopSku': params['shopSku']},
                                                     {"$set": params}, upsert=True)
                        break
            # self.logger.info(f'success to get new inventory of goods {goods_code}')
        except Exception as e:
            self.logger.error(f'failed to get new inventory of goods {goods_code}')

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
            goods_code = self.product_stock.aggregate([
                {'$match': {'storeName': '义乌仓', 'GoodsStatus': {'$in': self.status + self.status1 + self.status2}}},
                # {'$match': {'storeName': '义乌仓', 'GoodsStatus': {'$in': ['停产', '停售']}}},
                {'$group': {'_id': {"goodscode": "$goodscode"}}},
                {"$project": {'_id': 0, 'goodsCode': "$_id.goodscode"}}
            ])

            # print(len(list(goods_code)))
            # goods_code = [{'goodsCode': '9C1026'}]
            pl = Pool(50)
            pl.map(self.get_data, goods_code)
            pl.close()
            pl.join()
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
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(worker.work())

    end = time.time()
    date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
    print(date + f' it takes {end - start} seconds')
