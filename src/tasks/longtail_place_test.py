#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure

import os
from src.services.base_service import CommonService


class Worker(CommonService):
    """
    worker template
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    def get_data_from_ibay(self):
        # 获取ibay数据库数据
        sql = "SELECT i.itemid,i.mubanid,i.selleruserid,i.sku,now()::DATE AS recordingtime " \
              "FROM ebay_item i " \
              "LEFT JOIN ebay_user e ON e.selleruserid = i.selleruserid " \
              "WHERE e.state1 =1 " \
              "AND i.country = 'CN' " \
              "AND i.listingtype='FixedPriceItem' " \
              "AND i.listingstatus IN ('Active') " \
              "AND (now()::TIMESTAMP - TO_TIMESTAMP(i.last_sold_datetime))>= INTERVAL'58 days' " \
              "AND (now()::TIMESTAMP - TO_TIMESTAMP(i.starttime))>= INTERVAL'58 days' "
        self.ibay_cur.execute(sql)
        data = self.ibay_cur.fetchall()
        return data


    def inster_data_to_ShopElf(self):
        # 连接普源数据库，插入数据
        data_ibay = self.get_data_from_ibay()
        sql_inster = "insert into ibay365_LongTail_OffShelf(itemid,mubanid,selleruserid,sku,recordingtime) values(%s,%s,%s,%s,%s)"
        for row in data_ibay:
            self.cur.execute(sql_inster, tuple(row))
            print('putting{}'.format(row))

    def exe_pro(self):
        # 执行存储过程
        sql_proce = "EXEC ibay365_LongTail_onshelf"
        self.cur.execute(sql_proce)

    def trans(self):
        self.inster_data_to_ShopElf()
        self.exe_pro()

    def work(self):
        try:
            self.trans()
        except Exception as why:
            self.logger.error('fail to count sku cause of {} '.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


