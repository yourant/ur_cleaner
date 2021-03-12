#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-30 15:07
# Author: turpure

import os
import datetime
from src.services.base_service import CommonService


class Fetcher(CommonService):
    """
    fetch developer sold detail from erp and put them into data warehouse
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.warehouse = 'mysql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.warehouse_cur = self.base_dao.get_cur(self.warehouse)
        self.warehouse_con = self.base_dao.get_connection(self.warehouse)

    def close(self):
        self.base_dao.close_cur(self.cur)
        self.base_dao.close_cur(self.warehouse_cur)

    def get_saler_suffix(self):
        sql = 'call oauth_userInfo'
        self.warehouse_cur.execute(sql)
        ret = self.warehouse_cur.fetchall()
        username_list = list()
        for item in ret:
            if item['username']:
                username = item['username']
            else:
                username = '无人'
            if item['store']:
                username_list.append(username + '/' + item['store'])
        return ','.join(username_list)

    def fetch(self, begin_date, end_date):
        sql = 'EXEC oauth_taskOfGoodsProfitInfoDetail %s, %s, %s'
        all_suffix = self.get_saler_suffix()
        # print(all_suffix)
        self.cur.execute(sql, (begin_date, end_date, all_suffix))
        ret = self.cur.fetchall()
        for row in ret:
            yield (row['nid'], row['orderDate'], row['saler'], row['storeName'], row['allWeight'], row['allQty'],
                   row['amt'], row['orderAmt'], row['shippingAmt'], row['shipDiscount'], row['feeAmt'],
                   row['expressFare'], row['insuranceAmount'], row['skuPackFee'], row['sku'], row['skuQty'],
                   row['skuWeight'], row['skuCostPrice'], row['skuAmt'], row['exchangeRate'], row['goodsCode'])

    def push(self, rows):
        # for row in rows:
        #     print(row)
        sql = ('insert into oauth_taskOfGoodsProfitInfoDetailTmp('
               'nid, orderDate, saler, storeName, allWeight, allQty, amt, orderAmt, shippingAmt,'
               'shipDiscount, feeAmt, expressFare, insuranceAmount, skuPackFee, sku, skuQty, '
               'skuWeight, skuCostPrice, skuAmt, exchangeRate, goodsCode)'
               'values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ')
        self.warehouse_cur.executemany(sql, rows)
        self.warehouse_con.commit()

    def clean(self):
        sql = 'DELETE from oauth_taskOfGoodsProfitInfoDetailTmp'
        self.warehouse_cur.execute(sql)
        self.warehouse_con.commit()

    def work(self):
        try:
            end_date = str(datetime.datetime.today() - datetime.timedelta(days=1))[:10]
            begin_date = str(datetime.datetime.today() - datetime.timedelta(days=10))[:10]
            self.clean()
            rows = self.fetch(begin_date, end_date)
            self.push(rows)
            self.logger.info('success to fetch goods order details between {} and {}'.format(begin_date, end_date))
        except Exception as why:
            self.logger.error('fail to fetch dev goods profit details of {}'.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == '__main__':
    worker = Fetcher()
    worker.work()
