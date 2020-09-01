#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-09-17 16:57
# Author: turpure

import os
from src.services.base_service import CommonService
import redis


class Updating(CommonService):

    def __init__(self):
        super().__init__()
        self.rd = redis.Redis(host='192.168.0.150', port='6379', db=0)
        self.base_name = 'mssql'
        self.warehouse = 'mysql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.warehouse_cur = self.base_dao.get_cur(self.warehouse)
        self.warehouse_con = self.base_dao.get_connection(self.warehouse)

    def close(self):
        self.base_dao.close_cur(self.cur)
        self.base_dao.close_cur(self.warehouse_cur)

    def get_products(self):
        sql = ( 'select productId from proCenter.joom_cateProduct '
               'union select productId from proCenter.joom_storeProduct')
        self.warehouse_cur.execute(sql)
        ret = self.warehouse_cur.fetchall()
        for row in ret:
            yield row['productId']

    def put_task_queue(self, product_id):
        data = ','.join(['update', product_id])

        self.rd.lpush('joom_task', data)

    def run(self):
        try:
            rows = self.get_products()
            for ele in rows:
                self.logger.info(f'putting {ele}')
                self.put_task_queue(ele)
        except Exception as why:
            self.logger.error(f'failed to put joom-product-update-tasks because of {why}')
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == '__main__':
    worker = Updating()
    worker.run()
