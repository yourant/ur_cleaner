#! usr/bin/env/python3
# coding:utf-8
# @Time: 2021-01-20 16:30
# Author: henry


import os
import re
import datetime
from src.services.base_service import CommonService
import requests
from multiprocessing.pool import ThreadPool as Pool

from pymongo import MongoClient

mongo = MongoClient('192.168.0.150', 27017)
mongodb = mongo['operation']
# col = mongodb['wish_products']
col = mongodb['shopify_products']


class Worker(CommonService):
    """
    worker template
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    def get_shopify_password(self):
        # sql = "SELECT apikey as api_key,hostname,password FROM [dbo].[S_ShopifySyncInfo] where hostname='speedacx'"
        sql = "SELECT apikey as api_key,hostname,password FROM [dbo].[S_ShopifySyncInfo]"
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def clean(self):
        col.delete_many({})
        self.logger.info('success to clear shopify product list')

    def get_products(self, row):
        password = row['password']
        suffix = row['hostname']
        api_key = row['api_key']
        endpoint = 'products.json'
        url_head = 'https://' + api_key + ':' + password + '@'
        url_body = suffix + '.myshopify.com/admin/api/2019-07/'
        url = url_head + url_body + endpoint
        try:
            while True:
                ret = dict()
                headers = dict()
                for i in range(2):
                    try:
                        response = requests.get(url, timeout=10)
                        ret = response.json()
                        headers = response.headers
                        break
                    except Exception as why:
                        self.logger.error(f' fail to get of products of {suffix} '
                                          f'page cause of {why} {i} times ')
                # print(ret)
                try:
                    pro_list = ret['products']
                except :
                    pro_list = []
                    break
                # 保存数据
                for item in pro_list:
                    # int 转换成 string
                    item['_id'] = str(item['id'])
                    item['id'] = str(item['id'])
                    item['suffix'] = suffix
                    try:
                        item['image']['product_id'] = str(item['image']['product_id'])
                    except:
                        pass
                    try:
                        item['image']['variant_ids'] = [str(i) for i in item['image']['variant_ids']]
                    except:
                        pass
                    try:
                        item['image']['id'] = str(item['image']['id'])
                    except:
                        pass
                    for ele in item['variants']:
                        ele['id'] = str(ele['id'])
                        ele['product_id'] = str(ele['product_id'])
                        ele['image_id'] = str(ele['image_id'])
                        ele['inventory_item_id'] = str(ele['inventory_item_id'])
                    for ele in item['options']:
                        ele['id'] = str(ele['id'])
                        ele['product_id'] = str(ele['product_id'])
                    for ele in item['images']:
                        ele['id'] = str(ele['id'])
                        ele['product_id'] = str(ele['product_id'])
                        ele['variant_ids'] = [str(i) for i in ele['variant_ids']]

                    # print(item)
                    self.put(item)

                # 判断是否有下一页
                link_list = headers['Link'].split(',')
                link = ''
                for item in link_list:
                    arr = item.split(';')
                    if arr[1] == ' rel="next"':
                        link = arr[0].replace('<', '').replace('>', '')
                if link:
                    url = link.replace('https://', url_head)
                else:
                    break

        except Exception as e:
            self.logger.error(e)

    @staticmethod
    def put(row):
        # col.save(row)
        col.update_one({'_id': row['_id']}, {"$set": row}, upsert=True)

    def work(self):
        try:
            tokens = self.get_shopify_password()
            self.clean()
            pl = Pool(2)
            pl.map(self.get_products, tokens)
            pl.close()
            pl.join()
        except Exception as why:
            self.logger.error('fail to count sku cause of {} '.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


