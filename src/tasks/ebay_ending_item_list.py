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
        self.product_list = self.get_mongo_collection('operation', 'ebay_products')
        self.task = self.get_mongo_collection('operation', 'ebay_off_shelf_task')
        self.suffix_token = self.get_ebay_suffix_token()
        self.status = ['线下清仓', '停产', '清仓', '线上清仓', '线上清仓50P', '线上清仓100P', '停售']  # 下架状态

    def close(self):
        self.base_dao.close_cur(self.cur)

    def get_ebay_suffix_token(self):
        res = dict()
        sql = ('SELECT  NoteName AS suffix,EuSellerID AS storeName, MIN(EbayTOKEN) AS token '
               'FROM [dbo].[S_PalSyncInfo] WHERE SyncEbayEnable=1 '
               'and notename in (select dictionaryName from B_Dictionary '
               "where  CategoryID=12 and FitCode ='eBay' and used = 0) "
               "GROUP BY NoteName,EuSellerID ORDER BY NoteName ;")
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            res[row['suffix']] = row['token']
        return res

    def get_products(self, item_id):
        rows = self.product_list.find({'itemID': item_id,
                                      # 'parent_sku': {'$regex': goods_code},
                                      # 'suffix': suffix,
                                      # "id": "5f962aaeb0d3c2003d8e091d"
                                      }, no_cursor_timeout=True)
        for row in rows:
            if 'variations' in row and 'variation' in row['variations']:
                for item in row['variations']['variation']:
                    new_sku = item['sku'].split("@")[0]
                    # new_sku = new_sku.split("*")[0]
                    yield {'code': row['parentSku'], 'goods_code': row['goods_code'], 'shopSku': item['sku'],
                           'sku': new_sku, 'item_id': item_id, 'suffix': row['suffix'],
                           'quantity': item['quantity']}
            else:
                yield {'code': row['parentSku'], 'goods_code': row['goods_code'], 'shopSku': row['parentSku'],
                       'sku': row['goods_code'], 'item_id': item_id, 'suffix': row['suffix'],
                       'quantity': row['quantity']}

    def get_data(self, row):
        item_id = row['itemID']
        goods_code = row['goods_code']
        token = self.suffix_token[row['suffix']]
        try:
            product_list = self.get_products(item_id)
            goods_stock = self.product_stock.find({'goodscode': goods_code, 'storeName': '义乌仓',
                                                   'GoodsStatus': {'$in': self.status}})
            goods_stock_list = list(goods_stock)
            # print(goods_stock_list)
            product_list_num = 0
            product_list_status_num = 0
            for rw in product_list:
                # 如果存在SKU 在线数量大于 0 则 跳出循环
                if rw['quantity'] > 0:
                    product_list_num = 0
                    break
                product_list_num = product_list_num + 1
                for sku in goods_stock_list:
                    if sku['SKU'] == rw['sku']:
                        product_list_status_num = product_list_status_num + 1
            # print((goods_code, product_list_num, product_list_status_num))
            if product_list_num > 0 and product_list_num == product_list_status_num:
                self.task.update_one({'item_id': item_id},
                                     {"$set": {'item_id': item_id, 'suffix': row['suffix'], 'accessToken': token,
                                               'status': '初始化', 'created': datetime.datetime.today(),
                                               'executedResult': '', 'executedTime': ''}}, upsert=True)
        except Exception as e:
            self.logger.error(f'failed to get off shelf goods {goods_code} cause of {e}')

    def work(self):
        try:
            # 获取在线总数量为 0 的在线listing
            # product = self.product_list.find({'quantity': 0, 'itemID': '184652456097'})
            product = self.product_list.find({'quantity': 0, 'endTime': {'$gte': datetime.datetime.today()}})
            pl = Pool(50)
            pl.map(self.get_data, product)
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
