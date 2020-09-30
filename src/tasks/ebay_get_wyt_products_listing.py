import datetime
import time
from configs.config import Config
from src.services.base_service import CommonService
import requests
import json
import os
from bs4 import BeautifulSoup
import math
from src.services import oauth_wyt as wytOauth


class FetchEbay(CommonService):
    def __init__(self):
        super().__init__()
        self.base_url = "http://openapi.winit.com.cn/openapi/service"
        self.app_key = Config().get_config('gucang')['app_key']
        self.token = Config().get_config('gucang')['token']
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    # 爬取海外仓UK的产品数据
    def get_data(self, plat):
        # print(plat)
        step = 100
        if plat == 'uk':
            data = {
                "warehouseID": "1000069",
                "warehouseCode": "UK0001",
                "inReturnInventory": "Y",
                "isActive": "Y",
                "pageSize": str(step),
                "pageNum": "1"
            }

        elif plat == 'au':
            data = {
                "warehouseID": "1000001",
                "warehouseCode": "EWD",
                "inReturnInventory": "Y",
                "isActive": "Y",
                "pageSize": str(step),
                "pageNum": "1"
            }
        else:
            data = {
                "warehouseID": "1000001",
                "warehouseCode": "EWD",
                "inReturnInventory": "Y",
                "isActive": "Y",
                "pageSize": str(step),
                "pageNum": "1"
            }
        action = 'queryWarehouseStorage'
        try:
            oauth = wytOauth.Wyt()
            params = oauth.get_request_par(data, action)
            res = requests.post(self.base_url, json=params)
            ret = json.loads(res.content)
            if ret['code'] == 0:
                rows = self._parse_response(ret['data']['list'])
                self.save_data(rows, plat)
                if ret['data']['total'] >= step:
                    page = math.ceil(ret['data']['total'] / step)
                    for i in range(2, page + 1):
                        data['pageNum'] = str(i)
                        params = oauth.get_request_par(data, action)
                        response = requests.post(self.base_url, json=params)
                        result = json.loads(response.content)
                        rows = self._parse_response(result['data']['list'])
                        self.save_data(rows, plat)
        except Exception as e:
            self.logger.error('failed cause of {}'.format(e))

    def _parse_response(self, rows):
        try:
            for item in rows:
                print(item)
                # yield (item['skuCode'], item['weight'], item['length'], item['width'], item['height'])
                yield (item['productCode'], item['productWeight'], item['producLenght'], item['productWidth'], item['productHeight'])
        except Exception as e:
            self.logger.error('Failed to get sku storage detail cause of {}'.format(e))

    def save_data(self, rows, plat):
        try:
            # /print(123)
            if plat == 'au':
                sql = f'insert into AU_Storehouse_WeightAndSize values(%s,%s,%s,%s,%s)'
                self.cur.executemany(sql, rows)
                self.con.commit()
            elif plat == 'uk':
                sql = f'insert into UK_Storehouse_WeightAndSize values(%s,%s,%s,%s,%s)'
                self.cur.executemany(sql, rows)
                self.con.commit()
            else:
                pass
        except Exception as why:
            self.logger.error(f"fail to save sku size info in wyt warehouse cause of {why} ")

    def clean(self):
        uk_sql = "truncate table UK_Storehouse_WeightAndSize"
        au_sql = "truncate table AU_Storehouse_WeightAndSize"
        self.cur.execute(au_sql)
        self.cur.execute(uk_sql)
        self.con.commit()
        self.logger.info('success to clear sku size info in wyt warehouse')

    def run(self):
        begin_time = time.time()
        try:
            # self.clean()
            plat = ['au', 'uk']
            for item in plat:
                self.get_data(item)

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
