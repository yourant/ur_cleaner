#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-19 16:26
# Author: turpure


import time
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt
from ebaysdk.trading import Connection as Trading
from ebaysdk import exception
from src.services.base_service import BaseService
from configs.config import Config
import requests
import json


class VovaFee(BaseService):
    """
    fetch ebay fee using api
    """
    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')




    def get_vova_token(self):
        sql = 'SELECT top 1 AliasName AS suffix,MerchantID AS selleruserid,APIKey AS token FROM [dbo].[S_SyncInfoVova] WHERE SyncInvertal=0;'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_vova_fee(self, token):

        ret = self._get_vova_fee(token)
        print(ret)
        #     for fee in self._parse_response(ret, token):
        #         yield fee



    def _get_vova_fee(self, token):
        param = {
            "token": token['token'],
            # 'order_goods_sn ':'041ef7101f93afa8-1'
            # "since": '2020-04-01T00:00:00.000Z',
            # "limit": 100
        }
        url = f'https://merchant-api.vova.com.hk/v1/Order/ChangedOrders?token=' + token['token']
        # url = 'https://merchant-api.vova.com.hk/v1/Order/ChangedAddressOrders'
        # url = 'https://merchant-api-t.vova.com.hk/v1/Order/order'
        # url = "https://merchant.vova.com.hk/api/v1/Order/ShippingCarrierList?token=" + token['token']
        response = requests.get(url)
        print(url)
        print(response)
        ret = response.json()
        print(ret)
            # total_page = ret['page_arr']['totalPage']
            # rows = self.deal_products(token, ret['product_list'])
            # await asyncio.ensure_future(self.save(rows, token, page=1))
            # await asyncio.gather(asyncio.ensure_future(self.save(rows, token, page=1)))
            # if total_page > 1:
            #     for page in range(2, total_   page + 1):
            #         param['conditions']['page_arr']['page'] = page
            #         try:
            #             response = await session.post(url, data=json.dumps(param))
            #             res = await response.json()
            #             res_data = self.deal_products(token, res['product_list'])
            #             # await asyncio.ensure_future(self.save(res_data, token, page))
            #             await asyncio.gather(asyncio.ensure_future(self.save(res_data, token, page)))
            #         except Exception as why:
            #             self.logger.error(f'error while requesting page {page} cause of {why}')

    def _parse_response(self, ret, token):
        for row in ret:
            fee_type = row.AccountDetailsEntryType
            if fee_type not in ('FeeFinalValue', 'FeeFinalValueShipping',
                                'PayPalOTPSucc', 'PaymentCCOnce',
                                'PaymentCC', 'CreditFinalValue', 'CreditFinalValueShipping', 'Unknown'
                                ):
                fee = dict()
                fee['feeType'] = fee_type
                fee['Date'] = str(row.Date)
                fee['value'] = row.NetDetailAmount.value
                fee['currency'] = row.NetDetailAmount._currencyID
                fee['accountName'] = token['notename']
                fee['ItemID'] = row.ItemID
                if float(row.NetDetailAmount.value) >= 10 or float(row.NetDetailAmount.value) <= -10:
                    self.logger.warning('%s:%s' % (fee_type, float(row.NetDetailAmount.value)))
                if float(row.NetDetailAmount.value) != 0:
                    yield fee

    # def save_data(self, row):
    #     sql = 'insert into y_fee(notename,fee_type,total,currency_code,fee_time,batchId)' \
    #           ' values(%s,%s,%s,%s,%s,%s)'
    #     try:
    #         self.cur.execute(sql, (row['accountName'], row['feeType'], row['value'], row['currency'],
    #                          row['Date'], self.batch_id))
    #         self.logger.info("putting %s" % row['accountName'])
    #         self.con.commit()
    #     except pymssql.IntegrityError as e:
    #         pass
    #     except Exception as e:
    #         self.logger.error("%s while trying to save data" % e)
    #
    # def save_trans(self, token):
    #     ret = self.get_ebay_fee(token)
    #     for row in ret:
    #         self.save_data(row)

    def run(self):
        try:
            tokens = self.get_vova_token()
            for token in tokens:
                # time.sleep(1)
                self.get_vova_fee(token)
            # with ThreadPoolExecutor(16) as pool:
            #     future = {pool.submit(self.get_vova_fee, token): token for token in tokens}
            #     for fu in as_completed(future):
            #         try:
            #             data = fu.result()
            #             for row in data:
            #                 # print(row)
            #                 self.save_data(row)
            #         except Exception as e:
            #             self.logger.error(e)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.close()


if __name__ == '__main__':
    worker = VovaFee()
    worker.run()




