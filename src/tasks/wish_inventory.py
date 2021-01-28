#! usr/bin/env/python3
# coding:utf-8
# Author: turpure

import os
from multiprocessing.pool import ThreadPool as Pool
from src.services.base_service import CommonService
import requests


class Worker(CommonService):
    """
    worker
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    def calculate_quantity(self):
        # 计算
        procedure = ("B_wish_ModifyOnlineNumberOnTheIbay365 "
                     "'线下清仓', "  # 改0
                     "'爆款,旺款,浮动款,Wish新款,在售',"  # 改固定数量
                     "'停产,清仓,线上清仓,线上清仓50P,线上清仓100P,春节放假,停售'"  # 改实际库存
                     )
        self.cur.execute(procedure)
        self.con.commit()

    def get_wish_token_count(self):
        # 查询
        sql = "select count(*) as num from ibay365_wish_quantity where ISNULL(flag,0)=0 "
        self.cur.execute(sql)
        ret = self.cur.fetchone()
        return ret['num']

    def get_wish_token(self):
        # 查询
        sql = "select top 100 nid,token,sku,quantity,itemid,suffix,storage from ibay365_wish_quantity where ISNULL(flag,0)=0 "
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def update_inventory(self, row):
        print(row)
        token = row['token']
        sku = row['sku']
        inventory = row['quantity']
        headers = {'content-type': 'application/json', 'Authorization': 'Bearer ' + token}
        base_url = 'https://merchant.wish.com/api/v2/variant/update-inventory'
        param = {
            "sku": sku,
            "inventory": inventory
        }

        for i in range(2):
            try:
                response = requests.get(base_url, params=param, headers=headers, timeout=20)
                ret = response.json()
                print(ret)
                if ret["code"] == 0:
                    self.logger.info(f'success { row["suffix"] } to update { row["itemid"] }')
                    break
            except Exception as why:
                self.logger.error(f'fail to update inventory cause of  {why} and trying {i + 1} times')

    def update_flag(self, nid):
        sql = "update ibay365_wish_quantity set flag = 1 where nid=%"
        self.cur.execute(sql, nid)
        self.con.commit()

    def work(self):
        try:
            self.calculate_quantity()

            tokens = self.get_wish_token()
            pl = Pool(16)
            pl.map(self.update_inventory, tokens)
        except Exception as why:
            self.logger.error('fail to update wish inventory cause of {} '.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


