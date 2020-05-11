#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-09-17 16:57
# Author: turpure

from src.services.base_service import BaseService
from multiprocessing.pool import ThreadPool as Pool
import requests
import json
import asyncio
import datetime


class Uploading(BaseService):
    def __init__(self):
        super().__init__()

    def clean(self):
        sql = 'truncate table ibay365_vova_list'
        self.cur.execute(sql)
        self.con.commit()

    def get_vova_token(self):

        sql = 'SELECT AliasName AS suffix,MerchantID AS selleruserid,APIKey AS token FROM [dbo].[S_SyncInfoVova] WHERE SyncInvertal=0;'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        return ret

    def get_products(self, token):
        url = 'https://merchant.vova.com.hk/api/v1/product/productList'
        param = {
            "token": token['token'],
            "conditions": {
                "goods_state_filter": "",
                "middle_east_sale": "",
                "goods_promotion": "",
                "ban_status": "",
                "upload_batch_id": 0,
                "search": {
                    "search_type": "",
                    "search_value": ""
                },
                "page_arr": {
                    "perPage": 100,
                    "page": 1
                }
            }
        }
        try:
            with requests.session() as session:
                response = session.post(url, data=json.dumps(param))
                ret = response.json()
                total_page = ret['page_arr']['totalPage']
                rows = self.deal_products(token, ret['product_list'])
                self.save(rows, token, page=1)
                if total_page > 1:
                    for page in range(2, total_page + 1):
                        param['conditions']['page_arr']['page'] = page
                        try:
                            response = session.post(url, data=json.dumps(param))
                            res = response.json()
                            res_data = self.deal_products(token, res['product_list'])
                            self.save(res_data,token, page)
                        except Exception as why:
                            self.logger.error(f'error while requesting page {page} cause of {why}')
        except Exception as why:
            self.logger.error(f'error while requesting page 1 of {token["token"]} cause of {why}')

    @staticmethod
    def deal_products(token, rows):
        for row in rows:
            for item in row['sku_list']:
                index = item['goods_sku'].find('@#')
                new_sku = item['goods_sku'][0:index] if(index >= 0) else item['goods_sku']
                yield (row['parent_sku'], item['goods_sku'],  new_sku,
                row['product_id'], token['suffix'], token['selleruserid'], item['storage'], str(datetime.datetime.today())[:19])

    def save(self, rows, token, page):
        sql = 'insert into ibay365_vova_list (code,sku,newsku,itemid,suffix,selleruserid,storage,updateTime) values (%s,%s,%s,%s,%s,%s,%s,%s)'
        self.cur.executemany(sql, rows)
        self.con.commit()
        self.logger.info(f"success to save data page {page} in multi processing way of suffix {token['suffix']} ")

    def run(self):
        try:
            self.clean()
            pool = Pool()
            pool.map(self.get_products, self.get_vova_token())
            pool.close()
            pool.join()

        except Exception as why:
            self.logger.error(f'failed to put vova-get-product-tasks because of {why}')
        finally:
            self.close()


if __name__ == '__main__':

    import time
    start = time.time()
    worker = Uploading()
    worker.run()
    end = time.time()
    date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
    print(date + f' it takes {end - start} seconds')

