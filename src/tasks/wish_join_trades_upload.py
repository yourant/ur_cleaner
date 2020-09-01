#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-19 10:01
# Author: turpure

"""
upload tracking number of wish's merged trades
"""

import os
import requests
import json
from src.services.base_service import CommonService
from concurrent.futures import ThreadPoolExecutor


class WishUploader(CommonService):

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    def get_trades_info(self):
        sql = 'www_wish_join_trades_upload'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def upload_track_number(self, trades_info):
        modify_url = 'https://china-merchant.wish.com/api/v2/order/modify-tracking'
        upload_url = 'https://china-merchant.wish.com/api/v2/order/fulfill-one'
        id = trades_info['transactionid']
        data = {
            'access_token': trades_info['accesstoken'],
            'format': 'json',
            'id': id,
            'tracking_provider': trades_info['CarrierEN'],
            'tracking_number': trades_info['trackno'],
        }

        try:
            res = requests.post(upload_url, data=data)
            res_dict = json.loads(res.content)
            code = res_dict['code']
            if not code:
                self.logger.info('success to upload %s' % id)
                return True
            else:
                self.logger.info('failed to upload %s %s' % (id, res_dict))
                mo_res = requests.post(modify_url, data=data)
                mo_res_dict = json.loads(mo_res.content)
                mo_code = mo_res_dict['code']
                if not mo_code:
                    self.logger.info('success to modify %s' % id)
                    return True
                else:
                    self.logger.info('failed to modify %s %s' % (id, mo_res_dict))
        except Exception as e:
            self.logger.warn('failed to upload %s %s' % (id, e))

    def update_shipping_status(self, trade_info):
        trade_id = trade_info['nid']
        table_name = trade_info['tablename']
        sql = 'update %s set shippingmethod=1 where nid=%s'
        cur = self.cur
        try:
            cur.execute(sql, (table_name, trade_id))
            self.logger.info('ship %s' % trade_id)
            self.con.commit()

        except Exception as e:
            self.logger.debug('failed to ship %s cause of %s' % (trade_id, e))

    def upload_trans(self, trade_info):
        ret = self.upload_track_number(trade_info)
        if ret:
            self.update_shipping_status(trade_info)
            self.logger.info('upload %s successfully' % trade_info['nid'])

    def run(self):
        try:
            pool = ThreadPoolExecutor()
            pool.map(self.upload_trans, self.get_trades_info())
        except Exception as e:
            self.logger.error(e)
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == '__main__':
    uploader = WishUploader()
    uploader.run()


