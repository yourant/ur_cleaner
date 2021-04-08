#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure

import os
from src.services.base_service import CommonService
from configs.config import Config
import requests
from concurrent.futures import ThreadPoolExecutor as Pool
import json


class Worker(CommonService):
    """
    push wish template
    """
    def __init__(self):
        super().__init__()
        config = Config().config
        self.token = config['ur_center']['token']
        self.op_token = config['op_center']['token']
        self.base_name = 'mssql'
        self.warehouse = 'mysql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.warehouse_cur = self.base_dao.get_cur(self.warehouse)
        self.warehouse_con = self.base_dao.get_connection(self.warehouse)
        self.col = self.get_mongo_collection('operation', 'wish_template')

    def close(self):
        self.base_dao.close_cur(self.cur)
        self.base_dao.close_cur(self.warehouse_cur)

    def get_products(self):
        sql = ("SELECT gi.id FROM `proCenter`.`oa_goodsinfo` `gi` LEFT JOIN `proCenter`.`oa_goods` `g` ON g.nid = gi.goodsId WHERE  (`picStatus` = '已完善') AND (`completeStatus` LIKE '%wish%') and goodsStatus in ('爆款','旺款','浮动款','Wish新款','在售') and length(ifnull(requiredKeywords,'')) >19  and devDatetime >= date_sub(curdate(),interval 7 day)")
        self.warehouse_cur.execute(sql)
        ret = self.warehouse_cur.fetchall()
        for ele in ret:
            yield ele['id']

    def get_data_by_id(self, product_id):
        base_url = 'http://127.0.0.1:8089/v1/oa-goodsinfo/plat-export-wish-data'
        headers = {'content-type': 'application/json', 'Authorization': 'Bearer ' + self.token}
        data = json.dumps({"condition": {"id": product_id}})
        try:
            ret = requests.post(base_url, data=data, headers=headers)
            templates = ret.json()['data']['data']
            for tm in templates:
                tm['inventory'] = int(tm['inventory'])
                self.push(tm, product_id)
            self.logger.info(f'success to save  template of {product_id}')
        except Exception as why:
            self.logger.error(f'failed to  push template of {product_id} cause of {why}')

    def push(self, data, product_id):
        base_url = 'http://127.0.0.1:18881/v1/operation/wish-publish-trans-save'
        headers = {'content-type': 'application/json', 'Authorization': 'Bearer ' + self.op_token}
        body = json.dumps({"condition": data})
        try:
            ret = requests.post(base_url, data=body, headers=headers)
            content = ret.json()
            code = content['code']
            if code != 200:
                self.logger.error(f'failed to save  template of {data["sku"]} cause of {content["message"]}')
                raise Exception(f'{content["message"]}')
            else:
                self.logger.info(f'success to save  template of {data["sku"]}')

        except Exception as why:
            self.logger.error(f'failed to  push template of {data["sku"]} cause of {why}')
            raise Exception(f'{why}')

    # def push(self, data):
    #     self.col.save(data)

    def work(self):
        try:
            products = self.get_products()

            with Pool(32) as pl:
                pl.map(self.get_data_by_id, products)
        except Exception as why:
            self.logger.error('fail to push wish template  cause of {} '.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


