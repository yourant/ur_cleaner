#! usr/bin/env/python3
# coding:utf-8
# @Time: 2021-01-08 13:58
# Author: turpure

from src.tasks.a_basic_task import Worker


class StockWorker(Worker):
    """
    获取每个SKU的库存变动记录
    """

    # 出现在配置文件的数据库名称
    def __init__(self, databases=None):
        super(StockWorker, self).__init__(databases)

    def get_data(self):
        pass

    def put_data(self):
        pass

    def run(self):
        pass


if __name__ == '__main__':
    worker = StockWorker(databases=['mssql', 'erp'])
    worker.work()


