import datetime
import time
from configs.config import Config
from src.services.base_service import CommonService
import requests
import json
import os
from bs4 import BeautifulSoup
import math


class FetchEbay(CommonService):
    def __init__(self):
        super().__init__()
        self.base_url = "https://oms.goodcang.net/default/svc/web-service"
        self.app_key = Config().get_config('gucang')['app_key']
        self.token = Config().get_config('gucang')['token']
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    def get_products_from_good_cang(self, page_size, page):
        try:
            request_data = {'pageSize': page_size, 'page': page}
            params = json.dumps(request_data)
            body = ('<?xml version="1.0" encoding="UTF-8"?>' +
                    '<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://schemas.xmlsoap.org/soap/envelope/" xmlns:ns1="http://www.example.org/Ec/">' +
                    '<SOAP-ENV:Body>' +
                    '<ns1:callService>' +
                    '<paramsJson>' + params + '</paramsJson>' +
                    '<appToken>' + self.token + '</appToken>' +
                    '<appKey>' + self.app_key + '</appKey>' +
                    '<language>zh_CN</language>' +
                    '<service>getProductSkuList</service>' +
                    '</ns1:callService>' +
                    '</SOAP-ENV:Body>' +
                    '</SOAP-ENV:Envelope>')

            headers = {'content-type': 'text/xml; charset=UTF-8'}
            res = requests.post(self.base_url, data=body, headers=headers)

            soup = BeautifulSoup(res.text, features='xml')
            # 将 XML 数据转化为 Dict
            data = dict([(item.name, item.text) for item in soup.find_all('response')])
            response = data['response']
            data = json.loads(response)
            if data['message'] == 'Success':
                return data
            else:
                self.logger.info(data['message'])
                return []
        except Exception as e:
            self.logger.info(e)
            return []

    def get_products(self):
        page_size = 100
        try:
            data = self.get_products_from_good_cang(page_size, page = 1)
            for row in data['data']:
                if (row['Product_real_length'] or row['Product_real_width'] or
                        row['Product_real_height'] or row['Product_real_weight']):
                    res_list = (row['product_sku'], row['Product_real_weight'], row['Product_real_length'],
                                row['Product_real_width'], row['Product_real_height'])
                    self.insert(res_list)
            if data['count'] > page_size:
                page = math.ceil(data['count'] / page_size)
                for i in range(2, page + 1):
                    res = self.get_products_from_good_cang(page_size, i)
                    for item in res['data']:
                        if (item['Product_real_length'] > 0 or item['Product_real_width'] > 0 or
                                item['Product_real_height'] > 0 or item['Product_real_weight'] > 0):
                            res_list = (item['product_sku'], item['Product_real_weight'], item['Product_real_length'],
                                        item['Product_real_width'], item['Product_real_height'])
                            self.insert(res_list)
        except Exception as e:
            self.logger.info(e)

    def clean(self):
        clean = 'TRUNCATE TABLE UK_guCang_weightAndSize'
        self.cur.execute(clean)
        self.con.commit()
        self.logger.info('success to clean table UK_guCang_weightAndSize!')

    def insert(self, row):
        sql = 'insert into UK_guCang_weightAndSize(SKU,weight,length,width,height) values(%s,%s,%s,%s,%s)'
        try:
            self.cur.execute(sql, row)
            self.con.commit()
            self.logger.info(f'success to sync good cang uk products {row[0]}!')
        except Exception as why:
            self.logger.error(f"failed to sync good cang uk products cause of {why} ")

    def run(self):
        begin_time = time.time()
        try:
            self.clean()
            self.get_products()
        except Exception as e:
            self.logger.error(e)
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()
        print('程序耗时{:.2f}'.format(time.time() - begin_time))  # 计算程序总耗时


# 执行程序
if __name__ == "__main__":
    worker = FetchEbay()
    worker.run()
