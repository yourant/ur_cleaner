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
        self.task = self.get_mongo_collection('operation', 'wish_off_shelf_task')
        self.suffix_token = self.get_wish_suffix_token()
        self.status = ['线下清仓', '停产', '清仓', '线上清仓', '线上清仓50P', '线上清仓100P', '停售']  # 下架状态

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

    def get_data(self, row):
        goods_code = row['goodsCode']
        try:
            # 获取商品状态的SKU信息
            goods_stock = self.product_stock.find({'goodscode': goods_code, 'storeName': '义乌仓',
                                                   'GoodsStatus': {'$in': self.status}})
            goods_stock_list = list(goods_stock)

            # 获取在线listing
            products = self.product_list.find({"removed_by_merchant": "False", "review_status": "approved",
                                               'goods_code': goods_code,
                                               # 'parent_sku': {'$regex': goods_code},
                                               # 'suffix': suffix,
                                               # "id": "5f962aaeb0d3c2003d8e091d"
                                               }, no_cursor_timeout=True)
            for product in products:
                item_id = product['id']
                token = self.suffix_token[product['suffix']]

                product_list_num = 0
                product_list_status_num = 0
                for rw in product['variants']:
                    # 如果存在SKU 在线数量大于 0 则 跳出循环
                    if int(rw['Variant']['inventory']) > 0:
                        product_list_num = 0
                        break
                    product_list_num = product_list_num + 1
                    new_sku = rw['Variant']['sku'].split('@')[0]
                    for sku in goods_stock_list:
                        if sku['SKU'] == new_sku:
                            product_list_status_num = product_list_status_num + 1
                # print((item_id, goods_code, product_list_num, product_list_status_num))
                if product_list_num > 0 and product_list_num == product_list_status_num:
                    self.task.update_one({'item_id': item_id},
                                         {"$set": {'item_id': item_id, 'suffix': product['suffix'],
                                                   'accessToken': token, 'status': '初始化',
                                                   'created': datetime.datetime.today(),
                                                   'executedResult': '', 'executedTime': ''}}, upsert=True)
        except Exception as e:
            self.logger.error(f'failed to get ending list of goods {goods_code} cause of {e}')

    def work(self):
        try:
            goods_code = self.product_stock.aggregate([
                {'$match': {'storeName': '义乌仓', 'GoodsStatus': {'$in': self.status}}},
                # {'$match': {'storeName': '义乌仓', 'goodscode': '8A0111'}},
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

    end = time.time()
    date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
    print(date + f' it takes {end - start} seconds')
