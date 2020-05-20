#! usr/bin/env/python3
# coding:utf-8
# Author: turpure

from src.services.base_service import BaseService
import requests


class Worker(BaseService):
    """
    worker
    """

    def __init__(self):
        super().__init__()

    def get_joom_token(self):
        sql = 'select AccessToken from S_JoomSyncInfo'

        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_joom_refund_order(self):
        tokens = self.get_joom_token()
        for row in tokens:
            self.get_order(row)

    def get_order(selfi,row):
        token = row['accessToken']
        headers = {'content-type': 'application/json', 'Authorization': 'Bearer ' + token}
        base_url = 'https://api-merchant.joom.com/api/v2/order/fulfill-one'

    def save_refund_order(self):
        pass


    def do_something(self):
        pass

    def work(self):
        try:
            self.get_joom_token()
        except Exception as why:
            self.logger.error('fail to count sku cause of {} '.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


