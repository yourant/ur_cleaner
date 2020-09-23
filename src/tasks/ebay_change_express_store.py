#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-09-21 13:14
# Author: turpure


import os
from src.services.base_service import CommonService
import datetime


class Updater(CommonService):

    def __init__(self):
        super().__init__()
        self.all_orders = dict()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.all_logistics_ways = self.get_all_logistics_ways()
        self.express_mapping = self.get_express_mapping()

    def close(self):
        self.base_dao.close_cur(self.cur)

    def get_all_orders(self, order_time):
        """
        获取所有的UKLE缺货，UKMA有货的订单
        :param order_time:
        :return:
        """
        sql = 'exec ur_clear_ebay_adjust_express_to_change_store @orderTime=%s'
        self.cur.execute(sql, (order_time,))
        ret = self.cur.fetchall()
        for row in ret:
            row['newExpress'] = self.express_mapping[row['express']]
            yield row

    def get_express_mapping(self):
        sql = 'select UKLE, UKMA from ur_clear_ebay_adjust_express_store_express_mapping'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        out = dict()
        for row in ret:
            out[row['UKLE']] = row['UKMA']
        return out

    def get_all_logistics_ways(self):
        """
        获取所有的物流名称和物流ID
        :return:
        """
        sql = 'select name,nid from B_LogisticWay(nolock)'
        self.cur.execute(sql)
        out = dict()
        ret = self.cur.fetchall()
        for row in ret:
            out[row['name']] = row['nid']
        return out

    def set_order_new_store_express(self, order):
        """
        修改订单的物流信息和仓库信息
        :param order:
        :return:
        """
        try:

            # 改物流
            express_sql = f'update p_tradeun set logicsWayNid = {self.all_logistics_ways[order["newExpress"]]} where nid = {order["nid"]}'
            self.cur.execute(express_sql)

            # 改仓库
            store_sql = f'update p_tradeDtun set storeId = 54 where tradeNid = {order["nid"]}'
            self.cur.execute(store_sql)

            log_sql = f"exec S_WriteTradeLogs '{order['nid']}', '更改仓库和物流', 'ur_cleaner' "
            self.cur.execute(log_sql)

            # 提交
            self.con.commit()

            # 写日志
            self.logger.info(f'success to update {order["nid"]} of {order["suffix"]} set express to {order["newExpress"]}')
        except Exception as why:
            self.logger.info(f'fail to update {order["nid"]} of {order["suffix"]} cause of {why}')
            raise Exception(f'fail to set {order["nid"]} to {self.all_logistics_ways[order["newExpress"]]} ')

    def calculate_express_fee(self, order):
        # 计算运费和利润
        try:
            calculate_sql = f'exec P_Fr_CalcShippingCostByNid {order["nid"]}'
            log_sql = f"exec S_WriteTradeLogs  '{order['nid']}', '运费计算', 'ur_cleaner'"
            self.cur.execute(calculate_sql)
            self.cur.execute(log_sql)
        except Exception as why:
            self.logger.info(f'fail to update {order["nid"]} of {order["suffix"]} cause of {why}')
            raise Exception(f'fail to calculate express fee of {order["nid"]}')

    def transfer_to_handle(self, order):

        """
        缺货转至待派单
        :return:
        """
        # 获取批次号

        batch_number_sql = "exec  P_S_CodeRuleGet 150,'' "
        self.cur.execute(batch_number_sql)
        batch_number_ret = self.cur.fetchone()
        batch_number = batch_number_ret['MaxBillCode']

        # 插入任务池

        task_sql = (f"insert into temp_SelRecordNId (selNid, selCode, batchNo, selTime) values ({order['nid']},"
                     f"'ur_cleaner', '{batch_number}', GetDate() )")
        self.cur.execute(task_sql)

        # 根据批次号开始派单
        do_sql = f"P_TradeUn_ForwardPaiDan '{batch_number}','ur_cleaner'"

        self.cur.execute(do_sql)

        # 写派单日志

        log_sql = f"exec S_WriteTradeLogs '{order['nid']}', '转至派单', 'ur_cleaner' "

        self.cur.execute(log_sql)

        self.con.commit()

        self.logger.info(f'success to transfer {order["nid"]}')

    def set_log(self, order):
        """
        记录操作日志
        :param order:
        :return:
        """
        try:
            sql = 'INSERT INTO P_TradeLogs(TradeNID,Operator,Logs) VALUES (%s,%s,%s)'
            logs = ('ur_cleaner ' + str(datetime.datetime.today())[:19] + ' 更改物流方式为 ' + order['newName'])
            self.cur.execute(sql, (order['nid'], 'ur_cleaner', logs))
        except Exception as why:
            self.logger.error(f'fail to set log of {order["nid"]} cause of {why}')
            raise Exception(f'fail to set log of {order["nid"]}')

    def change_store_transaction(self, order):
        """"
        更改单个订单的仓库的事务
        """
        try:
            # 修改物流方式
            self.set_order_new_store_express(order)

            # 计算运费
            self.calculate_express_fee(order)

            # 转移到待派单
            self.transfer_to_handle(order)

        except Exception as why:
            self.logger.error(f'fail to change store  {order["nid"]} cause of {why}')

    def trans(self, order_time):
        """
        更改所有订单的仓库的事务
        :param order_time:
        :return:
        """
        orders = self.get_all_orders(order_time)
        for od in orders:
            self.change_store_transaction(od)

    def work(self):
        try:
            today = str(datetime.datetime.now())[:10]
            yesterday = str(datetime.datetime.now() - datetime.timedelta(days=1))[:10]
            for day in [today, yesterday]:
                self.trans(day)

        except Exception as why:
            self.logger.error(f'fail to finish task cause of {why}')
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


# 执行程序
if __name__ == "__main__":
    worker = Updater()
    worker.work()






