#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-09-17 16:57
# Author: turpure

import os
import aiohttp
import asyncio
from src.services.base_service import CommonService
from multiprocessing.pool import ThreadPool as Pool
import requests
import json
import datetime
import re


class OffShelf(CommonService):

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.task = self.get_mongo_collection('operation', 'vova_stock_task')
        self.product_stock = self.get_mongo_collection('operation', 'product_sku')

    def close(self):
        self.base_dao.close_cur(self.cur)

    def get_vova_token(self):
        sql = ("EXEC B_VovaOffShelfProducts  '停产,停售,线上清仓,线下清仓,线上清仓50P,线上清仓100P,春节放假'," +
              "'爆款,旺款,Wish新款,浮动款,在售,侵权,清仓'," +
              "'清仓,停产,停售,线上清仓,线上清仓50P,线上清仓100P,春节放假'")

        # sql = ("EXEC B_VovaOffShelfProducts  '停产,停售,春节放假'," +
        #        "'爆款,旺款,Wish新款,浮动款,在售,侵权'," +
        #        "'停产,停售,春节放假'")
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        return ret

    def update_products_storage(self, token):
        """
        1. 所有SKu都为0，就改成1
        2. 参见活动的产品，数量改为顾客指定数量
        3. 并发
        """
        url = 'https://merchant.vova.com.hk/api/v1/product/updateGoodsData'
        goods_info = {
            "product_id": token["item_id"],
            "goods_sku": token["shopSku"],
            "attrs": {
                "storage": int(token["targetInventory"])
            }
        }
        param = {
            "token": token['accessToken'],
            "goods_info": [goods_info]
        }
        headers = {'content-type': 'application/json'}
        try:
            for i in range(2):
                response = requests.post(url, data=json.dumps(param), headers=headers, timeout=20)
                ret = response.json()
                if ret['execute_status'] == 'success':
                    result = 'success'
                    self.update_task_status(token, result)
                    # self.logger.info(f'success {token["suffix"]} to update {token["item_id"]}')
                    break
                else:
                    if '存在被顾客预定' in ret['message']:
                        find_number = re.findall(r'存在被顾客预定(\d)件', ret['message'])
                        if find_number:
                            token['targetInventory'] = find_number[0]
                            self.update_products_storage(token)
                    if '标准库存不能全为0' in ret['message']:
                        self.disable_product(token)
                    if '克隆商品sku均价不能大于原型sku均价' in ret['message']:
                        if self.check_sku_status(token['goodsCode']):
                            self.disable_product(token)
                    else:
                        result = ret['message'] if 'message' in ret else 'failed'
                        self.update_task_status(token, result)
        except Exception as error:
            self.logger.error(f'fail to update products  of {token["shopSku"]} cause of {error}')

    def check_sku_status(self, goods_code):
        all_goods_status_num = self.product_stock.aggregate([
            {'$match': {'storeName': '义乌仓', 'goodscode': goods_code}},
            # {'$match': {'storeName': '义乌仓', 'GoodsStatus': {'$in': ['停产', '停售']}}},
            {'$group': {'_id': {"GoodsStatus": "GoodsStatus"}}},
            {"$project": {'_id': 0, 'goodsStatus': "$_id.GoodsStatus"}}
        ])
        filter_goods_status_num = self.product_stock.aggregate([
            {'$match': {'storeName': '义乌仓', 'goodscode': goods_code, 'GoodsStatus': {'$in': ['清仓', '停产', '停售']}}},
            # {'$match': {'storeName': '义乌仓', 'GoodsStatus': {'$in': ['停产', '停售']}}},
            {'$group': {'_id': {"GoodsStatus": "GoodsStatus"}}},
            {"$project": {'_id': 0, 'goodsStatus': "$_id.GoodsStatus"}}
        ])
        return True if len(list(all_goods_status_num)) == len(list(filter_goods_status_num)) else False

    def disable_product(self, token):
        item = {
                    "token": token['accessToken'],
                    "goods_list": [token['item_id']]
        }
        url = 'https://merchant.vova.com.hk/api/v1/product/disableSale'
        try:
            response = requests.post(url, data=json.dumps(item))
            res = response.json()
            # print(res)
            if res['execute_status'] == 'success':
                result = 'success'
            else:
                try:
                    result = res['data']['errors_list'][0]['message']
                except:
                    result = res['message'] if 'message' in res else 'failed'
            self.update_task_status(token, result)
            self.logger.info(f"{res['execute_status']} to disable product {token['item_id']}")
        except Exception as why:
            self.logger.error(f'fail to disable {token["item_id"]} casue of {why}')

    def update_task_status(self, row, res):
        row['status'] = 'success' if res == 'success' else 'failed'
        row['executedResult'] = res
        row['executedTime'] = str(datetime.datetime.today())[:19]
        self.task.update_one({'_id': row['_id']}, {"$set": row}, upsert=True)

    def run(self):
        try:
            # tokens = self.task.find({'status': '初始化'})
            # tokens = self.task.find({'item_id': '13818534'})
            tokens = self.task.find({'status': 'failed'})
            pl = Pool(16)
            pl.map(self.update_products_storage, tokens)
        except Exception as why:
            self.logger.error(f'failed to put vova-get-product-tasks because of {why}')
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == '__main__':
    import time
    start = time.time()
    worker = OffShelf()
    worker.run()
    end = time.time()
    date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
    print(date + f' it takes {end - start} seconds')
