#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-20 15:56
# Author: turpure

from src.services.base_service import CommonService
from ebaysdk.trading import Connection as Trading
import datetime
from configs.config import Config
from multiprocessing.pool import ThreadPool as Pool
from bson import ObjectId


# class AliSync(BaseService):
class AliSync(CommonService):
    """
    check purchased orders
    """

    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')
        self.col = self.get_mongo_collection('operation', 'vova_stock_task')
        self.product_list = self.get_mongo_collection('operation', 'vova_products')
        # self.base_name = 'mysql'
        # self.cur = self.base_dao.get_cur(self.base_name)
        # self.con = self.base_dao.get_connection(self.base_name)

    # def close(self):
    #     self.base_dao.close_cur(self.cur)

    def get_ebay_description(self):
        try:
            token = "AgAAAA**AQAAAA**aAAAAA**4cDcXQ**nY+sHZ2PrBmdj6wVnY+sEZ2PrA2dj6ADmYCiCJCKowydj6x9nY+seQ**kykBAA**AAMAAA**2raaq3ZjHDQ4DiKqgsIU8yUrmXhnO/E+Tr2d4L3iuN1gisy+zj98RKBw428kEtvZWwsStHqLx4la1EY3Dj0ZQnjr43xCp8Jnc8VCUDV5N4eN+E+LF6Rb6VxGVp8hKUAfugt7QnudjbDauQjCCUA9SDoYJzQ9u/rwRJ5QVEZucKSnvTdifZ8c0jChwZ/ef/qe3aUTpEObghcU597C/G47rfSp6bHH+hDaEyRVdfENahD/ysQRjZN4CG8C/XRSsgphCv0OqKx+/wK//68Yy7/fnG0vxJ75kceLFkFFSfILRB4afumjfHR9WG7yvgqXfAmkB0oppaSFWPZMv/mjRTfPaCjyP+ZeT6H+hKWTmnBnzJEcvdM3As8rcTgpEr9AXoGowK4I7LAle8WuIqLzCpvqpldIl4BUcrmipX2tngP/XBSE0UieQthBh3RUgBmAazaoZ+bVMMT9GKy8DpzZ/WbcirwI7YNCZNWMRIjJWCUJ8mv15baOXwvN3u9GtWqZRhi+m+xCHCQ45CbHTw1Y56Y8sJuZRnwmB8kpshRNXRBX6VZjeEW2prBnpIbmhHBeOubbPdB3EwEu6FKziSXgyK5tkdM/LDOnj6WRQGSHcNBhvt0pFFLrYOmoTqLgWRs9lC27ByG4IebXMWf1iTM3qvppvpEsPTzoBZywHT2tftQd/6iAi1O+ZcFMsziV+tpIy++KteSwyJuQY0hEV885RDDYplsgwbLnB+oBbL44p6iWgjckjLgjiqwOC8XrMiBWjC2S"
            api = Trading(config_file=self.config)
            trade_response = api.execute(
                'GetItem',
                {
                    'ItemID': 293716165258,
                    #     'SKU': row['Item']['SKU'],
                    #     # 'SKU': '7C2796@#01',
                    'requesterCredentials': {'eBayAuthToken': token},
                }
            )
            ret = trade_response.dict()
            print(ret)
            if ret['Ack'] == 'Success':
                return ret
            else:
                return []
        except Exception as e:
            self.logger.error(f"error cause of {e}")

    def update_data(self, item):
        # created = datetime.datetime.strptime(item['created'], "%Y-%m-%d %H:%M:%S")
        if not item['executedTime']:
            # updated = datetime.datetime.strptime(item['executedTime'], "%Y-%m-%d %H:%M:%S")
            updated = item['created'] + datetime.timedelta(hours=1)

        # print(created)
        # print(updated)
            print(updated)
            self.col.update_one({'_id': item['_id']},
                            {"$set": {'executedTime': updated}}, upsert=True)
                            # {"$set": {'created': created, 'executedTime': updated}}, upsert=True)

    def run(self):
        try:

            # update_time = str(datetime.datetime.today())[:19]
            # print(update_time)

            # products = self.col.find({'executedTime': {'$gte': '2021-04-01', '$lte': '2021-04-17 23:59:59'}})
            products = self.col.find({'status': 'failed'})
            # products = self.col.find({'_id': ObjectId('6075e33dea9d958f09ef7f93')})
            # products = self.col.find({'_id': ObjectId('60754a08ea9d958f09a6ecfd')})

            pl = Pool(100)
            pl.map(self.update_data, products)
            pl.close()
            pl.join()

            # for item in products:
            #     created = datetime.datetime.strptime(item['created'], "%Y-%m-%d %H:%M:%S")
                # print(datetime.datetime.today())
                # print(created)
                # self.col.insert_one(item)
                # self.col.update_one({'item_id': item['item_id']},{"$set": {'created': created}}, upsert=True)

        except Exception as e:
            self.logger(e)
        # finally:
        #     self.close()


if __name__ == '__main__':
    sync = AliSync()
    sync.run()
