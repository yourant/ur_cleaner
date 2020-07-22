#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-07-22 11:30
# Author: turpure

from src.services.base_service import BaseService


class Worker(BaseService):
    """
    worker template
    """

    def __init__(self):
        super().__init__()

    def do_something(self):
        pass

    def work(self):
        try:
            self.do_something()
        except Exception as why:
            self.logger.error('fail to finish task cause of {} '.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


