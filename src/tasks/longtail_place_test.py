#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure

from src.services.base_service import BaseService

class Worker(BaseService):
    """
    worker template
    """

    def __init__(self):
        super().__init__()

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
        sql_proce = 'EXEC ibay365_LongTail_onshelf'


    def trans(self):
        self.inster_data_to_ShopElf()
        self.exe_pro()

    def work(self):
        try:
            self.trans()
        except Exception as why:
            self.logger.error('fail to count sku cause of {} '.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


