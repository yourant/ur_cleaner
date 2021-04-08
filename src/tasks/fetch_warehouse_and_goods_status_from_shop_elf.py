#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-09-28 10:07
# Author: turpure

import os
import time
from src.services.base_service import CommonService
"""
同步仓库和商品状态到运营中心
"""


class Worker(CommonService):

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.col = self.get_mongo_collection('operation', 'product_dictionary')

    def close(self):
        self.base_dao.close_cur(self.cur)

    def sync_warehouse(self):
        try:
            sql = "SELECT StoreName FROM [dbo].[B_Store](nolock) WHERE Used=0;"
            self.cur.execute(sql)
            ret = self.cur.fetchall()
            for row in ret:
                self.col.insert_one({'type': '仓库', 'name': row['StoreName']})
            self.logger.info('success to sync store info')
        except Exception as why:
            self.logger.error(why)

    def sync_goods_status(self):
        try:
            sql = "SELECT DictionaryName FROM [dbo].[B_Dictionary](nolock) WHERE CategoryID=15 AND Used=0"
            self.cur.execute(sql)
            ret = self.cur.fetchall()
            for row in ret:
                self.col.insert_one({'type': '商品状态', 'name': row['DictionaryName']})
            self.logger.info('success to sync goods status info')
        except Exception as why:
            self.logger.error(why)

    def run(self):
        begin_time = time.time()
        try:
            self.col.delete_many({})
            # 同步仓库
            self.sync_warehouse()
            # 同步商品状态
            self.sync_goods_status()

        except Exception as why:
            self.logger.error(why)
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()
        print('程序耗时{:.2f}'.format(time.time() - begin_time))  # 计算程序总耗时


if __name__ == '__main__':
    worker = Worker()
    worker.run()
