#! usr/bin/env/python3
# coding:utf-8
# Author: turpure

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
        sql = "select itemid,sku,quantity,suffix,token from ibay365_joom_quantity"
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def update_inventory(self, row):
        token = row['token']
        sku = row['sku']
        inventory = row['quantity']
        headers = {'content-type': 'application/json', 'Authorization': 'Bearer ' + token}
        base_url = 'https://api-merchant.joom.com/api/v2/variant/update-inventory'
        try:
            for i in range(2):
                param = {
                    "sku": sku,
                    "inventory": inventory
                }
                response = requests.post(base_url, params=param, headers=headers, timeout=20)
                ret = response.json()
                if ret["code"] == 0:
                    # self.logger.info(f'success { row["suffix"] } to update { row["itemid"] }')
                    break
                else:
                    self.logger.error(f'fail to update inventory cause of  {ret["message"]} and trying {i} times')

        except Exception as e:
            self.logger.error(e)

    def work(self):
        try:
            tokens = self.get_joom_token()
            pl = Pool(16)
            pl.map(self.update_inventory, tokens)

        except Exception as why:
            self.logger.error('fail to update joom inventory cause of {} '.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


