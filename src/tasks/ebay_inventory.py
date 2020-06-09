#! usr/bin/env/python3
# coding:utf-8
# Author: turpure

from multiprocessing.pool import ThreadPool as Pool
from src.services.base_service import BaseService
from ebaysdk.trading import Connection as Trading
from ebaysdk import exception
from configs.config import Config


class Worker(BaseService):
    """
    worker
    """

    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')

    def get_ebay_token(self):

        # 计算
        procedure = ("EXEC B_ModifyOnlineNumberOfSkuOnTheIbay365"
                     "'',"  # 改0
                     "'爆款,旺款,Wish新款,浮动款,在售,清仓,停售',"  # 固定数量
                     "'停产,春节放假'"  # 真实数量
                     )
        self.cur.execute(procedure)
        self.con.commit()

        # 查询
        sql = "select itemid,sku,quantity,suffix,token from ibay365_quantity_online"
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def update_inventory(self, row):
        # itemId = row['itemid']
        # token = row['token']
        # sku = row['sku']
        # quantity = row['quantity']
        itemId = '312643919648'
        token = 'AgAAAA**AQAAAA**aAAAAA**SjdiXA**nY+sHZ2PrBmdj6wVnY+sEZ2PrA2dj6wDmYShCJCHogmdj6x9nY+seQ**kykBAA**AAMAAA**MNz0o3iH3lC145/wEPCAgzZqaZozXUhkOVCMY++OfahouA549EhMpuMHYZoZ9P2AKpKxX0E/pJMd1Qbhq5kJj/PA49aQTZLpMDaWwq2h5O28tsidgNV74AtHRfWpjwfwg0Y2L1QnM9/xCJIFobbp/XuOH2VP9su9lO1F8xI7OC9oBUWQc7eTiNSmYvrPAz+QfNyRYqrflQAsMSOqMeB7FoY5Gai7wm7cxpu+KRL9An9H5QrwCjLT8q8cM+PBK1HDizpGwJ89nf3vumRqz1HPwWxB7k62NJEWpKChxkNY45Klqll3QEOJaHfciOOOxucXkhXE1Pi+0ytM5FUa6q2/tOxwreS1GkZ/l1Yzzvj9iTLdWvy68UO+tp/jtUpE4yMGk2Z/GO22qMuzDZu1S4PM0OY9qtgTrRL+mIpiGpgsC2APHgBJNFhT9KbTRWCyv+EfkYEw9jVUxXJntqZTaKr93wadtd+3bM717RrmWocHMZfpD9zBTRYEM5aiFGP4Duha7uf/dn7MX/5w66wp8o7E9336nHTXpbsh1QOzLYDn9h+FqDqrHf6VvSFDGs6Eabv7K57rUErQoHYEuV2uaV1znRLXUhZRCVkkXSB0XwWnXSGh9Ma7eyJpHxmX6b99eHQtWUKAuQuCj9uC/NtoD+lo+BderkiMqwjY+2mhQKoR6KkLZom/MejqWxgg5hKP5ycnItuUxemK1vuFgzNBmrpMd04w69mIle0K/rFvituMy9yzaESBPghLbbZfLkKUSVrw'
        sku = '7F247101_XL@#01'
        quantity = '100'
        api = Trading(config_file=self.config)
        try:
            for i in range(1):
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
                    print(ret)
                    if ret["Ack"] == 'Success' or ret["Ack"] == 'Warning':
                        self.logger.info(f'success { row["suffix"] } to update { row["itemid"] }')
                        break
                    else:
                        self.logger.error(f'fail to update ebay inventory cause of  {ret["message"]} and trying {i} times')
                except exception.ConnectionError as e:
                    self.logger.error('Item {} connect to failed cause of {}'.format(itemId, e))
        except Exception as e:
            self.logger.error(e)

    def work(self):
        # try:
            # tokens = self.get_ebay_token()
            # pl = Pool(16)
            # pl.map(self.update_inventory, tokens)

            self.update_inventory(123)

        # except Exception as why:
        #     self.logger.error('fail to update ebay inventory cause of {} '.format(why))
        # finally:
        #     self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


