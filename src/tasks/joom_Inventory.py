#! usr/bin/env/python3
# coding:utf-8
# Author: turpure

import json
from multiprocessing.pool import ThreadPool as Pool
from src.services.base_service import BaseService
import requests


class Worker(BaseService):
    """
    worker
    """

    def __init__(self):
        super().__init__()

    def get_joom_token(self):
        sql = "select itemid,sku,quantity,suffix,token from ibay365_joom_quantity where itemid='5bd190221436d4017b860f7d'"
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_order(self, row):
        token = row['token']
        sku = row['sku']
        inventory = row['quantity']
        headers = {'content-type': 'application/json', 'Authorization': 'Bearer ' + token}
        base_url = 'https://api-merchant.joom.com/api/v2/variant/update-inventory'
        try:
            while True:
                param = {
                    "sku": sku,
                    "inventory": inventory
                }
                response = requests.post(base_url, params=param, headers=headers, timeout=20)
                ret = response.json()
                if ret["code"] == 0:
                    self.logger.info(f'success { row["suffix"] } to update { row["itemid"] }')
                    break
                else:
                    self.logger.error(f'fail { ret["message"] }')

        except Exception as e:
            self.logger.error(e)


    def work(self):
        try:
            tokens = self.get_joom_token()
            pl = Pool(16)
            pl.map(self.get_order, tokens)

        except Exception as why:
            self.logger.error('fail to count sku cause of {} '.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


