import datetime
import time
from src.services.base_service import CommonService
from configs.config import Config
from src.services import oauth_wyt as wytOauth
from pymongo import MongoClient
import math
import json
import requests
import os


# mongo = MongoClient('192.168.0.150', 27017)


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
        self.base_url = "http://openapi.winit.com.cn/openapi/service"
        self.end_time = str(datetime.datetime.today() - datetime.timedelta(days=1))[:10]
        self.begin_time = str(datetime.datetime.today() - datetime.timedelta(days=91))[:10]
        self.oauth = wytOauth.Wyt()

    def close(self):
        self.base_dao.close_cur(self.cur)
        self.base_dao.close_cur(self.warehouse_cur)

    # 查询头程服务
    def get_server_list(self):
        data = {
            # OW0101-标准海外仓入库
            "productType": "OW0101",
        }
        action = 'winit.wh.pms.getWinitProducts'
        try:
            params = self.oauth.get_request_par(data, action)
            res = requests.post(self.base_url, json=params)
            ret = json.loads(res.content)
            if ret['code'] == '0':
                return ret['data']
            else:
                self.logger.error('failed to get journey service cause of {}'.format(ret['msg']))
                return []
        except Exception as e:
            self.logger.error('failed to get journey service cause of {}'.format(e))
            return []

    def get_order_list(self, item):
        step = 100
        data = {
            # SD - 标准海外仓入库
            "orderType": "SD",
            # UKMA 仓库
            "destinationWareHouseCode": "UK0002",
            "orderCreateDateEnd": self.end_time,
            "orderCreateDateStart": self.begin_time,
            'winitProductCode': item[0],
            "pageParams": {
                "pageSize": str(step),
                "pageNo": "1"
            }
        }
        action = 'winit.wh.inbound.getOrderList'
        try:
            params = self.oauth.get_request_par(data, action)
            res = requests.post(self.base_url, json=params)
            ret = json.loads(res.content)
            if ret['code'] == '0':
                self.get_order_detail(ret['data']['orderList'], item[1])
                if ret['data']['pageParams']['totalCount'] > step:
                    page = math.ceil(ret['data']['pageParams']['totalCount'] / step)
                    for i in range(2, page + 1):
                        data['pageNum'] = str(i)
                        params = self.oauth.get_request_par(data, action)
                        response = requests.post(self.base_url, json=params)
                        result = json.loads(response.content)
                        self.get_order_detail(result['data']['orderList'], item[1])
        except Exception as e:
            self.logger.error('failed cause of {}'.format(e))

    def get_order_detail(self, rows, ship_type):
        for row in rows:
            # 筛选订单状态  OD-已下单 RE-已收货 TS-运输中 EWC-已到仓 PS-部分上架
            if row['status'] in ['OD', 'RE', 'TS', 'EWC', 'PS']:
                params_data = {
                    "orderNo": row['orderNo'],
                    "isIncludePackage": "Y",
                }
                action = 'winit.wh.inbound.getOrderDetail'
                try:
                    params = self.oauth.get_request_par(params_data, action)
                    res = requests.post(self.base_url, json=params)
                    ret = json.loads(res.content)
                    if ret['code'] == '0':
                        data = self._parse_response(ret['data']['merchandiseList'],
                                                    ret['data']['destinationWarehouseName'], ship_type)
                        # print(data)
                        self.save_data(data)
                except Exception as e:
                    self.logger.error('Failed to get sku storage detail cause of {}'.format(e))

    def _parse_response(self, rows, warehouse_name, ship_type):
        try:
            for row in rows:
                if row['actualQuantity'] == 0:
                    num = row['inspectionQty'] if row['inspectionQty'] > 0 else row['quantity']
                    yield (row['merchandiseCode'], num, '万邑通UK-MA仓', ship_type, str(datetime.datetime.today())[:19])
        except Exception as e:
            self.logger.error('Failed to get sku storage detail cause of {}'.format(e))

    def save_data(self, rows):
        sql = ('insert into cache_wyt_ukma_sku_not_in_store_num (sku,num,storeName,shipType,createdTime) ' +
               'values (%s,%s,%s,%s,%s)')
        try:
            self.warehouse_cur.executemany(sql, rows)
            self.warehouse_con.commit()
        except Exception as why:
            self.logger.error(f"fail to save sku storage in wyt warehouse cause of {why} ")

    def clean(self):
        sql = "truncate table cache_wyt_ukma_sku_not_in_store_num"
        self.warehouse_cur.execute(sql)
        self.warehouse_con.commit()
        self.logger.info('success to clear sku not in store storage in wyt ukma warehouse')

    def run(self):
        begin_time = time.time()
        try:
            self.clean()
            # server_list = self.get_server_list()
            # 海运散货   空运-普货
            server_list = dict({'OW01011004124': '海运散货', 'OW01011004133': '空运-普货'})

            for row in server_list.items():
                self.get_order_list(row)

        except Exception as e:
            self.logger.error(e)
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()
        print('程序耗时{:.2f}'.format(time.time() - begin_time))  # 计算程序总耗时


if __name__ == "__main__":
    worker = FetchEbayProductsStorage()
    worker.run()
