#! usr/bin/env/python3
# coding:utf-8
# Author: turpure

from multiprocessing.pool import ThreadPool as Pool
from src.services.base_service import CommonService
from ebaysdk.trading import Connection as Trading
from ebaysdk import exception
from configs.config import Config
import json
import os


class Worker(CommonService):
    """
    worker
    """

    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    def get_ebay_token(self):

        # 计算
        procedure = ("EXEC B_ModifyOnlineNumberOfSkuOnTheIbay365"
                     "'清仓,停产,停售,线上清仓,线下清仓,线上清仓50P,线上清仓100P',"     #改0
                     "'爆款,旺款,Wish新款,浮动款,在售',"                           #固定数量
                     "'清仓,停产,停售,线上清仓,线上清仓50P,线上清仓100P,春节放假'"      #真实数量

                     )
        self.cur.execute(procedure)
        self.con.commit()

        # 查询
        sql = "select itemid,sku,quantity,suffix,token from ibay365_quantity_online(nolock)"
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def update_inventory(self, row):
        itemId = row['itemid']
        token = row['token']
        sku = row['sku']
        quantity = row['quantity']
        # itemId = '132155850737'
        # token = 'AgAAAA**AQAAAA**aAAAAA**MT5iXA**nY+sHZ2PrBmdj6wVnY+sEZ2PrA2dj6AGkIunC5iKpwudj6x9nY+seQ**kykBAA**AAMAAA**G94BGIR5+h1zIqLVoA6KCsSjnznpyYvWjHcd69qp5mRYJl54latmHQTnvAbZrVt10rMjaSEgXgWDo/3D/Wt6/x9j1L0IMocHg4ZCMTkDKibSXmhvX8JOUDfuy5BqE7LSZB2m8ATQw4x7u2pt2zK2vF2ZC/Qd+CM3C6difqzC1CFkd3FaYpLg31ZC1yl/vsEyyTTcNHDxDzDHSjFMayIDQqV7ET4LvFFiLn+GVaf/KxCwJbsKjqauHkBxZFe0tu5XrKVHbM0g6rVNLkZnfZLbGWOMgTurThexwz8sxFhhHBFqokWZbsQeiOGGl95eOy0VFwhYIiVm2SJL38h+AevJ92lttjh0FMN3l2PxcNXouyaXH12jMYLRHusva7iHuu80Of+WnIJU4XyPUx00+Hr0P6ldu6g7IeEFSRY7Kz5+q+e1a/EQGb0ATKDXOSFxFyNteUZ51oOUhgro2caz26Bned8WJRPzhhvB1laad/YJGE9N4BEREat7g8g2CbtMmlX6DPjkCgMjHqD+Z/Ga8jW4d0TxyUmM6Nm31pHyV9CZgqUCjBz/rC447BieWUsPXHwYrqh8t5b6inohlcRiPtf1fHAm0xxcJYYWwYrCl4+VxjO12VVrIDQU7xTt3utnGXjj0y8w/TOUmNb3JY1sECACrgwVLBjzhBvnCURYn2qoIFD3hQR48m7Z6xny+IN+ENwb/jKkO66NHzk6bV6SlLFae7ZhwP1+ifPI7nELDhSFfGjTCZdZrZ5ScIkEdx9awlB7'
        # sku = '2C013904@#11'
        # quantity = '19'
        api = Trading(config_file=self.config)
        try:
            for i in range(2):
                try:
                    response = api.execute(
                        'ReviseFixedPriceItem',
                        {
                            'Item':{
                                'ItemID':itemId,
                                'Variations':{
                                    'Variation':{
                                        'SKU':sku,
                                        'Quantity':quantity
                                    }
                                }
                            },
                            'requesterCredentials': {'eBayAuthToken': token},
                        }
                    )
                    res = response.json()
                    ret = json.loads(res)
                    if ret["Ack"] == 'Success' or ret["Ack"] == 'Warning':
                        self.logger.info(f'success { row["suffix"] } to update { row["itemid"] }')
                        # self.logger.info(f'success')
                        break
                    else:
                        self.logger.error(f'fail to update Item {itemId} cause of  {ret["Errors"]["ShortMessage"]} and trying {i} times')
                except exception.ConnectionError as e:
                    self.logger.error('Item {} connect to failed cause of {}'.format(itemId, e))
        except Exception as e:
            self.logger.error('Item {} update failed cause of {}'.format(itemId, e))

    def work(self):
        try:
            tokens = self.get_ebay_token()
            pl = Pool(16)
            pl.map(self.update_inventory, tokens)

            # self.update_inventory(123)

        except Exception as why:
            self.logger.error('fail to update ebay inventory cause of {} '.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


