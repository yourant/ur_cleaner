#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure


import os
import re
import math
import time
import datetime
from src.services.base_service import CommonService
import requests
from multiprocessing.pool import ThreadPool as Pool

from pymongo import MongoClient

mongo = MongoClient('192.168.0.150', 27017)

table = mongo['operation']['wish_products']
stock = mongo['wish']['wish_sku_stock']


class Sync(CommonService):
    """
    sync
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.status = ['线下清仓']   #改0
        self.status1 = ['爆款', '旺款', '浮动款', 'Wish新款', '在售']  # 改固定数量
        self.status2 = ['停产', '清仓', '线上清仓', '线上清仓50P', '线上清仓100P', '春节放假', '停售'] # 改实际库存'

    def close(self):
        self.base_dao.close_cur(self.cur)

    def get_wish_token(self):
        sql = ("SELECT AccessToken,aliasname FROM S_WishSyncInfo WHERE  "
               "aliasname is not null"
               " and  AliasName = 'WISE126-southkin' "
               " and  AliasName not in "
               "(select DictionaryName from B_Dictionary where CategoryID=12 and used=1 and FitCode='Wish') ")
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            print(row)
            yield row

    def sync_sku_stock(self):
        stock.delete_many({})
        sql = ("SELECT gs.sku,shopsku,GoodsSKUStatus AS status,isnull(sk.hopeUseNum,0) as hopeUseNum "
               "FROM B_GoodsSKU(nolock) gs INNER JOIN B_Goods(nolock) as g on g.nid = gs.goodsid "
               "LEFT JOIN Y_R_tStockingWaring(nolock) as sk on sk.sku = gs.sku AND storeName='义乌仓' "
               "LEFT JOIN (SELECT DISTINCT shopsku,sku FROM B_GoodsSKULinkShop(nolock) ) s ON s.sku=gs.sku")
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            row['hopeUseNum'] = str(int(row['hopeUseNum']))
            stock.insert_one(row)

    def get_data(self, row):
        # print(row)
        token = row['AccessToken']
        suffix = row['aliasname']
        products = self.get_products(suffix)
        # print(len(list(products)))
        for product in products:
            # print(product)
            sku_info = stock.find({'sku': {'$regex': product['sku']}})
            print(len(sku_info))
            for sku in sku_info:
                storage = int(product['storage'])
                hope_use_num = int(sku['hopeUseNum'])
                print(sku)
                check = self.check(storage, hope_use_num, sku['status'])
                # 判断sku数量是否需要修改
                # if not check:
                #     break



                print(check)
            break

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

    @staticmethod
    def pull():
        # rows = col.find({'sku':{'$regex':"8C1085"}})
        # rows = table.find()
        rows = table.find({"removed_by_merchant": "False", "review_status": "approved"})
        for row in rows:
            yield (row['code'], row['sku'], row['newSku'], row['itemid'], row['suffix'], row['selleruserid'],
                   row['storage'], row['listingType'], row['country'], row['paypal'], row['site'], row['updateTime'])

    @staticmethod
    def get_products(suffix):
        rows = table.find({'suffix': suffix, "removed_by_merchant": "False"
                              # , "review_status": "approved"
                              , 'parent_sku': {'$regex': '7N0828'}
                           })
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
                yield {'sku': ele['newsku'], 'itemid': ele['itemid'], 'storage': ele['storage'], 'suffix': ele['suffix']}

    def push_db(self, rows):
        try:
            rows = list(rows)
            number = len(rows)
            step = 100
            end = math.ceil(number / step)
            for i in range(0, end):
                value = ','.join(map(str, rows[i * step: min((i + 1) * step, number)]))
                sql = f'insert into ibay365_wish_lists(code, sku, newsku,itemid, suffix, selleruserid, storage, updateTime) values {value}'
                try:
                    self.cur.execute(sql)
                    self.con.commit()
                    self.logger.info(
                        f"success to save data of wish products from {i * step} to  {min((i + 1) * step, number)}")
                except Exception as why:
                    self.logger.error(f"fail to save data of wish products cause of {why} ")
        except Exception as why:
            self.logger.error(f"fail to save wish products cause of {why} ")

    def save_trans(self):
        # rows = self.get_products()
        # self.push_db(rows)
        # rows = self.pull()
        rows = self.get_products()
        self.push_db(rows)
        mongo.close()

    def work(self):
        begin = time.time()
        try:
            self.sync_sku_stock()

            tokens = self.get_wish_token()
            pl = Pool(50)
            pl.map(self.get_data, tokens)
            pl.close()
            pl.join()
            # self.save_trans()

        except Exception as why:
            self.logger.error(why)
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()
            mongo.close()
        print('程序耗时{:.2f}'.format(time.time() - begin))  # 计算程序总耗时


if __name__ == "__main__":
    worker = Sync()
    worker.work()
