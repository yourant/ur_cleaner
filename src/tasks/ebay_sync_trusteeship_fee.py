#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-08-20 16:26
# Author: henry

import os
import datetime
from src.services.base_service import CommonService
from configs.config import Config
import phpserialize


class EbayFee(CommonService):
    """
    fetch ebay fee using api
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    def save_ebay_order_fee_to_y_fee(self, begin, end):
        sql = "EXEC oauth_update_fixed_fee_for_hosting_account %s,%s"
        self.cur.execute(sql, (begin, end))
        self.con.commit()

    def run(self):
        try:
            today = str(datetime.datetime.today())
            begin = str(datetime.datetime.today() - datetime.timedelta(days=7))[:10]
            end = str(today)[:10]
            # print(begin, end)
            self.save_ebay_order_fee_to_y_fee(begin, end)
            self.logger.info('success to sync ebay trusteeship fee between {} and {}'.format(begin, end))
        except Exception as e:
            self.logger.error(e)
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == '__main__':
    worker = EbayFee()
    worker.run()
