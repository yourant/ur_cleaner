#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-09-28 10:07
# Author: turpure

import os
import time
from src.services.base_service import CommonService
"""
库存预警。
"""


class Worker(CommonService):

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.col_product = self.get_mongo_collection('operation', 'product_sku')
        self.total_num = 10000000
        self.step = 1000

    def close(self):
        self.base_dao.close_cur(self.cur)

    def sync_stocking_waring(self):
        sql = "EXEC Y_R_KC_StockingWaringAll '',0,0,'','0','','','',100000000,1,'','0','','',''"
        self.cur.execute(sql)
        self.con.commit()
        self.logger.info('success to sync product stock info to shop elf')

    def sync_stocking_waring_to_ur_operation_center(self):

        try:
            # sql = "SELECT * FROM Y_R_tStockingWaring(nolock) where rowid between %s and %s"
            sql = "SELECT * FROM Y_R_tStockingWaring(nolock)"
            self.cur.execute(sql)
            ret = self.cur.fetchall()
            for row in ret:
                del row['SelFlag']
                del row['rowid']
                del row['fpagecount']
                del row['RecCount']
                del row['LinkUrl2']
                del row['LinkUrl3']
                del row['LinkUrl4']
                del row['LinkUrl5']
                del row['LinkUrl6']
                del row['onroadamount']
                del row['OutCode']
                del row['WarningCats']
                del row['ModelNum']
                row['MinPrice'] = 0 if row['MinPrice'] is None else float(row['MinPrice'])
                row['goodsPrice'] = 0 if row['goodsPrice'] is None else float(row['goodsPrice'])
                row['costprice'] = 0 if row['costprice'] is None else float(row['costprice'])
                row['costmoney'] = 0 if row['costmoney'] is None else float(row['costmoney'])
                row['CanSellDay'] = 0 if row['CanSellDay'] is None else float(row['CanSellDay'])
                row['maxnum'] = 0 if row['maxnum'] is None else int(row['maxnum'])
                row['minnum'] = 0 if row['minnum'] is None else int(row['minnum'])
                row['Number'] = 0 if row['Number'] is None else int(row['Number'])
                row['ReservationNum'] = 0 if row['ReservationNum'] is None else int(row['ReservationNum'])
                row['usenum'] = 0 if row['usenum'] is None else int(row['usenum'])
                row['UnPaiDNum'] = 0 if row['UnPaiDNum'] is None else int(row['UnPaiDNum'])
                row['hopeUseNum'] = 0 if row['hopeUseNum'] is None else int(row['hopeUseNum'])
                row['NotInStore'] = 0 if row['NotInStore'] is None else int(row['NotInStore'])
                row['MinPrice'] = 0 if row['MinPrice'] is None else float(row['MinPrice'])
                row['SaleReNum'] = 0 if row['SaleReNum'] is None else float(row['SaleReNum'])
                row['SuggestNum'] = 0 if row['SuggestNum'] is None else float(row['SuggestNum'])
                row['DayNum'] = 0 if row['DayNum'] is None else float(row['DayNum'])
                row['GoodsCostMoney'] = 0 if row['GoodsCostMoney'] is None else float(row['GoodsCostMoney'])

                self.col_product.insert_one(row)
        except Exception as why:
            self.logger.error(why)

    def run(self):
        begin_time = time.time()
        try:
            # 计算库存预警数据
            # self.sync_stocking_waring()
            # 同步库存预警到运营中心
            self.col_product.delete_many({})

            # tasks = list()
            # for i in range(0, 1000):
            #     tasks.append(i)
            # # print(tasks)
            # pl = Pool(16)
            # pl.map(self.sync_stocking_waring_to_ur_operation_center, tasks)
            # pl.close()
            # pl.join()
            self.sync_stocking_waring_to_ur_operation_center()

        except Exception as why:
            self.logger.error(why)
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()
        print('程序耗时{:.2f}'.format(time.time() - begin_time))  # 计算程序总耗时


if __name__ == '__main__':
    worker = Worker()
    worker.run()
