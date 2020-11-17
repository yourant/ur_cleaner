#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-11-02 14:30
# Author: turpure

import os
from src.services.base_service import CommonService


class Worker(CommonService):
    """
    worker template
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    def get_paypal_toke(self):
        pass

    def get_refund(self):
        pass

    def insert(self):
        pass

    def trans(self):
        pass

    def work(self):
        try:
            self.trans()
        except Exception as why:
            self.logger.error('fail to finish task cause of {} '.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


