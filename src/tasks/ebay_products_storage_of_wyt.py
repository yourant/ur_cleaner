import datetime
import time
from src.services.base_service import CommonService
from configs.config import Config
from src.services import oauth_wyt as wytOauth
import math
import json
import requests
import os


class FetchEbayProductsStorage(CommonService):
    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')
        self.base_name = 'mssql'
        self.warehouse = 'mysql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.warehouse_cur = self.base_dao.get_cur(self.warehouse)
        self.warehouse_con = self.base_dao.get_connection(self.warehouse)

    def close(self):
        self.base_dao.close_cur(self.cur)
        self.base_dao.close_cur(self.warehouse_cur)

    def getData(self):
        base_url = "http://openapi.winit.com.cn/openapi/service"
        step = 100
        data = {
            "warehouseID": "1000069",
            "warehouseCode": "UK0001",
            "inReturnInventory": "Y",
            "isActive": "Y",
            "pageSize": str(step),
            "pageNum": "1"
        }
        action = 'queryWarehouseStorage'
        try:
            oauth = wytOauth.Wyt()
            params = oauth.get_request_par(data, action)
            res = requests.post(base_url,json=params)
            ret = json.loads(res.content)
            if ret['code'] == 0:
                rows = self._parse_response(ret['data']['list'])
                self.save_data(rows)
                if ret['data']['total'] >= step:
                    page = math.ceil(ret['data']['total']/step)
                    for i in range(2, page + 1):
                        data['pageNum'] = str(i)
                        params = oauth.get_request_par(data, action)
                        response = requests.post(base_url, json=params)
                        result = json.loads(response.content)
                        rows = self._parse_response(result['data']['list'])
                        self.save_data(rows)
        except Exception as e:
            self.logger.error('failed cause of {}'.format(e))

    def _parse_response(self, rows):
        try:
            for row in rows:
                yield (row['productName'],row['productCode'], row['qtyAvailable'], row['warehouseName'], row['warehouseCode'], row['warehouseID'], str(datetime.datetime.today())[:19])
        except Exception as e:
            self.logger.error('Failed to get sku storage detail cause of {}'.format(e))

    def save_data(self, rows):
        sql = f'insert into cache_wyt_sku_storage(skuName,sku,inventory,warehouseName,warehouseCode,warehouseID,updateTime) values (%s,%s,%s,%s,%s,%s,%s)'
        try:
            self.warehouse_cur.executemany(sql,rows)
            self.warehouse_con.commit()
        except Exception as why:
            self.logger.error(f"fail to save sku storage in wyt warehouse cause of {why} ")

    def get_wyt_warehouse(self):
        pass

    def clean(self):
        sql = "truncate table cache_wyt_sku_storage"
        self.warehouse_cur.execute(sql)
        self.warehouse_con.commit()
        self.logger.info('success to clear sku storage in wyt warehouse')

    def run(self):
        beginTime = time.time()
        try:
            self.clean()
            self.getData()
        except Exception as e:
            self.logger.error(e)
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()
        print('程序耗时{:.2f}'.format(time.time() - beginTime))  # 计算程序总耗时


if __name__ == "__main__":
    worker = FetchEbayProductsStorage()
    worker.run()






