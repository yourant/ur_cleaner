#! usr/bin/env/python3
# coding:utf-8
# Author: turpure

import os
from multiprocessing.pool import ThreadPool as Pool
from src.services.base_service import CommonService
import requests
import datetime


class Worker(CommonService):
    """
    worker
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.task = self.get_mongo_collection('operation', 'joom_stock_task')

    def close(self):
        self.base_dao.close_cur(self.cur)

    def get_joom_token(self):

        # 计算
        procedure = ("EXEC B_joom_ModifyOnlineNumberOnTheIbay365"
                     "'线下清仓',"  # 改0
                     "'爆款,旺款,Wish新款,浮动款,在售',"  # 固定数量
                     "'停产,春节放假,线上清仓,线上清仓50P,线上清仓100P,清仓,停售'"  # 真实数量
                     )
        self.cur.execute(procedure)
        self.con.commit()

        # 查询
        sql = "select itemid,sku,quantity,suffix,token from ibay365_joom_quantity(nolock)"
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def update_inventory(self, row):
        token = row['accessToken']
        sku = row['shopSku']
        inventory = row['targetInventory']
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
                    row['status'] = 'success'
                    row['executedResult'] = 'success'
                    row['executedTime'] = str(datetime.datetime.today())[:19]
                    self.task.update_one({'_id': row['_id']}, {"$set": row}, upsert=True)
                    self.logger.info(f'success { row["suffix"] } to update { row["item_id"] }')
                    break
                else:
                    row['status'] = 'failed'
                    row['executedResult'] = 'failed'
                    row['executedTime'] = str(datetime.datetime.today())[:19]
                    self.task.update_one({'_id': row['_id']}, {"$set": row}, upsert=True)

        except Exception as e:
            self.logger.error(e)

    def work(self):
        try:
            tokens = self.task.find({'status': '初始化'})
            # tokens = self.task.find({'status': '初始化', 'item_id': '6051a144ce140001069f6175'})
            pl = Pool(16)
            pl.map(self.update_inventory, tokens)

        except Exception as why:
            self.logger.error('fail to update joom inventory cause of {} '.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


