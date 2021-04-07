import datetime
import time
from configs.config import Config
from src.services.base_service import CommonService
import requests
import json
import os
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
    def get_data(self):
        step = 100
        data = {
            # "skuCode": "UK-A009801",
            "pageSize": str(step),
            "pageNo": "1"
        }
        action = 'winit.mms.item.list'
        try:
            oauth = wytOauth.Wyt()
            params = oauth.get_request_par(data, action)
            res = requests.post(self.base_url, json=params)
            ret = json.loads(res.content)
            # print(ret)
            if ret['code'] == '0':
                self._parse_response(ret['data']['list'])
                if ret['data']['pageParams']['totalCount'] > step:
                    page = math.ceil(ret['data']['pageParams']['totalCount'] / step)
                    for i in range(2, page + 1):
                        data['pageNo'] = str(i)
                        params = oauth.get_request_par(data, action)
                        response = requests.post(self.base_url, json=params)
                        result = json.loads(response.content)
                        self._parse_response(result['data']['list'])
        except Exception as e:
            self.logger.error('failed cause of {}'.format(e))

    def _parse_response(self, rows):
        update_time = str(datetime.datetime.today())[:19]
        try:
            for row in rows:
                res_list = (row['skuCode'], row['registerWeight'], row['registerLength'],
                            row['registerWidth'], row['registerHeight'], update_time)
                for item in row['customsDeclarationList']:
                    if item['countryCode'] == 'UK':
                        sql = f'insert into UK_Storehouse_WeightAndSize values(%s,%s,%s,%s,%s,%s)'
                        self.cur.execute(sql, res_list)
                        self.con.commit()
                        break
                    if item['countryCode'] == 'AU':
                        sql = f'insert into AU_Storehouse_WeightAndSize values(%s,%s,%s,%s,%s,%s)'
                        self.cur.execute(sql, res_list)
                        self.con.commit()
                        break
        except Exception as e:
            self.logger.error('Failed to get sku storage detail cause of {}'.format(e))

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
            self.get_data()
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
