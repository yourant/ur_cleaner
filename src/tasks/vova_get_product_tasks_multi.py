#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-09-17 16:57
# Author: turpure

from src.services.base_service import BaseService
# from multiprocessing.pool import ThreadPool as Pool
from multiprocessing import Manager
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor
import requests
import json
import datetime


class Uploading(BaseService):

    def __init__(self, token=None):
        super().__init__()
        self.token = token

    def clean(self):
        sql = 'truncate table ibay365_vova_list'
        self.cur.execute(sql)
        self.con.commit()
        self.logger.info('success to clear vova listing')

    def get_vova_token(self):

        sql = 'SELECT AliasName AS suffix,MerchantID AS selleruserid,APIKey AS token FROM [dbo].[S_SyncInfoVova](nolock) WHERE SyncInvertal=0;'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        self.logger.info('success to get vova token')
        return ret

    def get_products(self, token, lock):
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
                self.save(rows, token, 1, lock)
                if total_page > 1:
                    for page in range(2, total_page + 1):
                        param['conditions']['page_arr']['page'] = page
                        try:
                            response = session.post(url, data=json.dumps(param))
                            res = response.json()
                            res_data = self.deal_products(token, res['product_list'])
                            self.save(res_data,token, page, lock)
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

    def save(self, rows, token, page, lock):
        try:
            lock.acquire()
            sql = 'insert into ibay365_vova_list (code,sku,newsku,itemid,suffix,selleruserid,storage,updateTime) values (%s,%s,%s,%s,%s,%s,%s,%s)'
            self.cur.executemany(sql, list(rows))
            self.con.commit()
            self.logger.info(f"success to save data page {page} in multi processing way of suffix {token['suffix']} ")
            lock.release()
        except Exception as why:
            self.logger.error(f"fail to save data page {page} in multi processing way of suffix {token['suffix']} cause of {why} ")

    def run(self):
        try:
            self.clean()
            tokens = self.get_vova_token()
            with ThreadPoolExecutor() as pool:
                pool.map(self.get_products, tokens)

        except Exception as why:
            self.logger.error(f'failed to put vova-get-product-tasks because of {why}')
        finally:
            self.close()


def start_work(args):
    token = args['token']
    lock = args['lock']
    worker = Uploading(token)
    worker.get_products(token, lock)
    worker.close()

if __name__ == '__main__':

    import time
    start = time.time()
    worker = Uploading()
    worker.clean()
    tokens = worker.get_vova_token()
    worker.close()
    lk = Manager().Lock()
    # with ThreadPoolExecutor() as pool:
    pool = Pool()
    pool.map(start_work, [{'token': token, 'lock': lk} for token in tokens])
    pool.close()
    pool.join()
    end = time.time()
    date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
    print(date + f' it takes {end - start} seconds')

