#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-09-17 16:57
# Author: turpure

from src.services.base_service import BaseService
import requests
import json
import re


class OffShelf(BaseService):
    def __init__(self):
        super().__init__()

    def get_vova_token(self):
        sql = ("EXEC B_VovaOffShelfProducts  '侵权,清仓,停产,停售,线上清仓,线下清仓,线上清仓50P,线上清仓100P'," +
              "'爆款,旺款,Wish新款,浮动款,在售'," +
              "'清仓,停产,停售,线上清仓,线上清仓50P,线上清仓100P,春节放假'")

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
            "product_id": token["itemid"],
            "goods_sku": token["sku"],
            "attrs": {
                "storage": token["storage"]
            }
        }
        param = {
            "token": token['token'],
            "goods_info": [goods_info]
        }
        try:
            response = requests.post(url, data=json.dumps(param))
            res = response.json()
            if res['execute_status'] == 'success':
                self.logger.info(f"success to off shelf vova product itemid:{token['itemid']},sku:{token['sku']}")
            else:
                if '存在被顾客预定' in res['message']:
                    find_number = re.findall(r'存在被顾客预定(\d)件', res['message'])
                    if find_number:
                        token['storage'] = find_number[0]
                        self.update_products_storage(token)
                if '标准库存不能全为0' in res['message']:
                    self.disable_product(token)

                else:
                    self.logger.error(f"failed to off shelf vova product itemid:{token['itemid']},"
                                      f"sku:{token['sku']} because of {res['message']}")
        except Exception as error:
            self.logger.error(f'fail to update products  of {token["sku"]} cause of {error}')

    def disable_product(self, token):
        item = {
                    "token": token['token'],
                    "goods_list": [token['itemid']]
        }
        url = 'https://merchant.vova.com.hk/api/v1/product/disableSale'
        try:
            response = requests.post(url, data=json.dumps(item))
            res = response.json()
            self.logger.info(f"{res['execute_status']} to disable product {token['itemid']}")
        except Exception as why:
            self.logger.error(f'fail to disable {token["itemid"]} casue of {why}')

    def run(self):
        try:
            tokens = self.get_vova_token()
            for token in tokens:
                try:
                    self.update_products_storage(token)
                except Exception as error:
                    self.logger.error(f'fail to update products  of {token["sku"]} cause of {error}')
        except Exception as why:
            self.logger.error(f'failed to put vova-get-product-tasks because of {why}')
        finally:
            self.close()


if __name__ == '__main__':

    import time
    start = time.time()
    worker = OffShelf()
    worker.run()
    end = time.time()
    print(f'it takes {end - start} seconds')
