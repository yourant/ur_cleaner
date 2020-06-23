#! usr/bin/env/python3
# coding:utf-8
# Author: turpure

# from multiprocessing.pool import ThreadPool as Pool
from concurrent.futures import ThreadPoolExecutor as Pool
from src.services.base_service import BaseService
from ebaysdk.trading import Connection as Trading
from ebaysdk import exception
from configs.config import Config
import json


class Worker(BaseService):
    """
    worker
    """

    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')

    def get_ebay_token(self):

        # 计算
        # procedure = ("EXEC B_ModifyOnlineNumberOfSkuOnTheIbay365Oversea"  # 实际库存为0的产品改0
        #              )
        # self.cur.execute(procedure)
        # self.con.commit()

        # 查询
        sql = "select itemid,sku,quantity,suffix,token from ibay365_quantity_online_oversea where itemId= '383262135881'"
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def update_inventory(self, row):
        itemId = row['itemid']
        token = row['token']
        sku = row['sku']
        quantity = row['quantity']
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
                    ret = response.json()
                    ret = json.loads(ret)
                    if ret["Ack"] == 'Success' or ret["Ack"] == 'Warning':
                        self.logger.info(f'success { row["suffix"] } to update { row["itemid"] }')
                        break
                    else:
                        self.logger.error(f'fail to update ebay inventory cause of  {ret["Errors"]["ShortMessage"]} and trying {i} times')
                except exception.ConnectionError as e:
                    self.logger.error('Item {} connect to failed cause of {}'.format(itemId, e))
        except Exception as e:
            self.logger.error(e)

    def work(self):
        try:
            tokens = self.get_ebay_token()
            with Pool(2) as pl:
                pl.map(self.update_inventory, tokens)

        except Exception as why:
            self.logger.error('fail to update ebay inventory cause of {} '.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


