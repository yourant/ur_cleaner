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
        # procedure = ("EXEC B_joom_ModifyOnlineNumberOnTheIbay365"
        #              "'',"  # 改0
        #              "'爆款,旺款,Wish新款,浮动款,在售,清仓,停售',"  # 固定数量
        #              "'停产,春节放假'"  # 真实数量
        #              )
        # self.cur.execute(procedure)
        # self.con.commit()

        # 查询
        sql = "select * from ibay365_wish_quantity"
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
                print(param)
                response = requests.get(base_url, params=param, headers=headers, timeout=20)
                ret = response.json()
                print(ret)
                if ret["code"] == 0:
                    self.logger.info(f'success { row["suffix"] } to update { row["itemid"] }')
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


