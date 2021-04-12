#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-20 15:56
# Author: turpure

from src.services.base_service import CommonService
from ebaysdk.trading import Connection as Trading
import datetime
from configs.config import Config


# class AliSync(BaseService):
class AliSync(CommonService):
    """
    check purchased orders
    """

    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')
        self.col = self.get_mongo_collection('operation', 'wish_stock_task')
        self.product_list = self.get_mongo_collection('operation', 'joom_products')
        self.test = self.get_mongo_collection('operation', 'joom_products')
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

    def run(self):
        try:
            # for i in range(33):
            #     begin = str(datetime.datetime.strptime('2020-08-01', '%Y-%m-%d') + datetime.timedelta(days=i))[:10]
            #     # print(begin)
            #     rows = self.get_data(begin)
            #     for row in rows:
            #         # print(row)
            #         col2.update_one({'recordId': row['recordId']}, {"$set": row}, upsert=True)
            #     self.logger.info(f'success to sync data in {begin}')
            # res = self.get_ebay_description()

            # update_time = str(datetime.datetime.today())[:19]
            # print(update_time)

            products = self.product_list.find({'goods_code': {'$in': [None]}})
            for item in products:
                goods_code = item['parent_sku'].split('@')[0]
                print(goods_code)
                # self.col.insert_one(item)
                self.product_list.update_one({'id': item['id'], 'parent_sku': item['parent_sku']},
                                             {"$set": {'goods_code': goods_code}}, upsert=True)
        except Exception as e:
            self.logger(e)
        # finally:
        #     self.close()


if __name__ == '__main__':
    sync = AliSync()
    sync.run()
