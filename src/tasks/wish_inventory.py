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

    def get_wish_token(self):

        # 计算
        procedure = ("B_wish_ModifyOnlineNumberOnTheIbay365 "
                     "'停产,清仓,线上清仓,线下清仓,线上清仓50P,线上清仓100P', " # 改0
                     "'爆款,旺款,浮动款,Wish新款,在售'," # 改固定数量
                     "'停产,清仓,线上清仓,线上清仓50P,线上清仓100P'" # 改实际库存
                     )
        self.cur.execute(procedure)
        self.con.commit()

        # 查询
        sql = "select   token, sku, quantity,itemid,suffix from ibay365_wish_quantity"
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def update_inventory(self, row):
        token = row['token']
        sku = row['sku']
        inventory = row['quantity']
        headers = {'content-type': 'application/json', 'Authorization': 'Bearer ' + token}
        base_url = 'https://merchant.wish.com/api/v2/variant/update-inventory'
        try:
            for i in range(2):
                param = {
                    "sku": sku,
                    "inventory": inventory
                }
                response = requests.get(base_url, params=param, headers=headers, timeout=20)
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
            # self.get_wish_token()
            tokens = self.get_wish_token()
            pl = Pool(16)
            pl.map(self.update_inventory, tokens)

        except Exception as why:
            self.logger.error('fail to update wish inventory cause of {} '.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


