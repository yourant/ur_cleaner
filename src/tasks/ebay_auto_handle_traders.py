#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-07-24 11:30
# Author: turpure


import os
import datetime
from src.services.base_service import BaseService


class Worker(BaseService):
    """
    ebay 自动派单
    """

    def __init__(self):
        super().__init__()
        self.batch_number = self.get_batch_number()

    def get_orders(self):
        """
        正常单
        地址异常单
        负利润单
        :return:
        """
        sql = 'ur_clear_ebay_auto_handle_orders'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def merger_orders(self):
        """
        合并订单
        :return:
        """
        pass

    def get_batch_number(self):
        """
        获得批次号
        :return:
        """
        sql = "exec  P_S_CodeRuleGet 170,''"
        self.cur.execute(sql)
        ret = self.cur.fetchone()
        return ret['MaxBillCode']

    def handle(self, order):
        """
        派单
        :param order:
        :return:
        """
        sql = (f"P_TR_TradeSaveLogisticWay @TradeNid={order['nid']},@EubFlag=0,@OrigFilterFlag=5,"
               f"@ExpressNID={order['expressNID']},@LogicWayNID={order['logicsWayNID']},"
               f"@TrackNo='{order['TrackNo']}',@StoreNID={order['StoreNID']},@BatchNum='{self.batch_number}',"
               f" @Operater ='ur_cleaner',@IsAutoPaiDan=1,@IsCNPL=0"
               )

        try:
            self.cur.execute(sql)
            self.logger.info(f'success to handle {order["nid"]}')
        except Exception as why:
            self.logger.error(f'fail to handle {order["nid"]} cause of {why}')

    def update_order_info(self, order):
        sql = 'update p_trade set BatchNum=%s where nid=%s'
        try:
            self.cur.execute(sql, (self.batch_number, order['nid']))
            self.logger.info(f'success to update info of {order["nid"]}')
        except Exception as why:
            self.logger.error(f'fail to update info of {order["nid"]} cause of {why}')

    def set_log(self, order):
        """
        记录操作日志
        :param order:
        :return:
        """
        sql = 'INSERT INTO P_TradeLogs(TradeNID,Operator,Logs) VALUES (%s,%s,%s)'
        try:
            logs = ('ur_cleaner ' + str(datetime.datetime.today())[:19] + ' 派单成功 ')
            self.cur.execute(sql, (order['nid'], 'ur_cleaner', logs))
            self.con.commit()
            # self.logger.info(f'success to set log of {order["nid"]}')
        except Exception as why:
            self.logger.error(f'fail to set log of {order["nid"]}')

    def trans(self):
        self.merger_orders()
        orders = self.get_orders()
        for od in orders:
            self.handle(od)
            self.update_order_info(od)
            self.set_log(od)

    def work(self):
        try:
            self.trans()
        except Exception as why:
            self.logger.error('fail to finish task cause of {} '.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


