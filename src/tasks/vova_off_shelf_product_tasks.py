#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-09-17 16:57
# Author: turpure

from src.services.base_service import BaseService
import requests
import json


class OffShelf(BaseService):
    def __init__(self):
        super().__init__()

    def get_vova_token(self):
        sql = ("EXEC B_VovaOffShelfProducts  '侵权,清仓,停产,停售,线上清仓,线下清仓,线上清仓50P,线上清仓100P'," +
              "'爆款,旺款,Wish新款,浮动款,在售'," +
              "'清仓,停产,停售,线上清仓,线上清仓50P,线上清仓100P,春节放假'")

        self.cur.execute(sql)
        ret = self.cur.fetchall()
        # ret = [
        #     {
        #         'token':'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE1NzQxNDc0NDUsInNjb3BlIjpbImdldCIsInBvc3QiXSwidWlkIjoiMzY1NDMiLCJ1TmFtZSI6IlB1eXVhbiJ9.BJtBIYwJ3O_OfSMeIIrQ3BDYWXs_iYCLuY5tMNXr_k0',
        #         'sku':'6C328903@#Q1',
        #         'storage':0,
        #         'itemid':'15449465'
        #     },
        #     {
        #         'token':'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE1NzQxNDc0NDUsInNjb3BlIjpbImdldCIsInBvc3QiXSwidWlkIjoiMzY1NDMiLCJ1TmFtZSI6IlB1eXVhbiJ9.BJtBIYwJ3O_OfSMeIIrQ3BDYWXs_iYCLuY5tMNXr_k0',
        #         'sku': '6C328906@#Q1',
        #         'storage': 0,
        #         'itemid': '15449465'
        #     }
        # ]
        return ret

    def off_shelf_products(self, token):
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
        response = requests.post(url, data=json.dumps(param))
        res = response.json()
        if res['execute_status'] == 'success':
            message = f"success to off shelf vova product itemid:{token['itemid']},sku:{token['sku']}"
        else:
            message = f"failed to off shelf vova product itemid:{token['itemid']},sku:{token['sku']} because of {res['message']}"
        self.logger.info(message)

    def run(self):
        try:
            tokens = self.get_vova_token()
            for token in tokens:
                self.off_shelf_products(token)
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
