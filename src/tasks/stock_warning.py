#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-09-28 10:07
# Author: turpure

import os
import time
from src.services.base_service import CommonService

"""
库存预警。
"""


class Worker(CommonService):

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    def run(self):
        begin_time = time.time()
        try:
            sql = "EXEC Y_R_KC_StockingWaringAll '',0,0,'','0','','','',900000,1,'','0','','',''"
            self.cur.execute(sql)
            self.con.commit()
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
