#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-10-10 16:12
# Author: turpure


from src.services.base_service import BaseService

import requests
import json


class Worker(BaseService):

    def get_products(self):
        base_url = 'http://111.231.88.85:38080/hysj_v2/wish_api/pb_tags?u_name=youran&time=1570762401&sign=60312dd9c60778b319b8a4b5a050b385'
        res = requests.post(base_url)
        ret = res.json()['result']
        for ele in ret:
            yield (ele['tag'], ele['product_num'], ele['pb_product_num'],
                   ele['max_pb_price'], ele['min_pb_price'], ele['avg_pb_price'],
                   ele['stat_time'], ele['reach_style']
                   )

    def save_products(self, rows):
        sql = ('insert into proEngine.wish_products (tag,product_num,pb_product_num,max_pb_price,'
               'min_pb_price,avg_pb_price,stat_time,reach_style) values ('
               '%s,%s,%s,%s,%s,%s,%s,%s)')
        self.warehouse_cur.executemany(sql, rows)
        self.warehouse_con.commit()

    def run(self):
        try:
            rows = self.get_products()
            self.save_products(rows)
            self.logger.info('success to get wish products from haiYing')
        except Exception as why:
            self.logger.error(f'fail to get wish products from haiYing cause of {why}')
        finally:
            self.close()


if __name__ == '__main__':
    worker = Worker()
    worker.run()
