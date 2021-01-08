#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-07-22 11:30
# Author: turpure

import os
from src.services.base_service import CommonService
from abc import ABCMeta, abstractmethod


class Worker(CommonService, metaclass=ABCMeta):
    """
    worker template
    """

    # 出现在配置文件的数据库名称
    def __init__(self, databases=None):
        super().__init__()
        if databases is None:
            databases = []
        self.db_pool = {}
        self.databases = databases
        for db in databases:
            self.db_pool[db] = {'con': self.base_dao.get_connection(db), 'cur': self.base_dao.get_cur(db)}

    def close(self):
        for db in self.databases:
            self.base_dao.close_cur(self.db_pool[db]['cur'])

    @abstractmethod
    def run(self):
        pass

    def work(self):
        try:
            self.run()
        except Exception as why:
            self.logger.error('fail to finish task cause of {} '.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker(['erp'])
    worker.work()


