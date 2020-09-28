#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-07-21 13:14
# Author: turpure


import os
import re
from src.services.base_service import CommonService
import math
from src.tasks.ebay_change_express_config import special_post_codes
import datetime


class Updater(CommonService):

    def __init__(self):
        super().__init__()
        self.all_orders = dict()
        self.rate = 0.22
        self.profit_money = 50
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.tracked_logistics_ways = self.get_tracked_ways()
        self.all_logistics_ways = self.get_all_logistics_ways()
        self.express_mapping = self.get_stores_express_mapping()


    def get_tracked_ways(self):
        """
        获取ebay海外仓的挂号物流名称
        :return:
        """
        sql = 'select name from ur_clear_ebay_adjust_express_tracked_ways(nolock)'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        out = list()
        for row in ret:
            out.append(row['name'])
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

    def get_stores_express_mapping(self):

        """
        获取不同仓库之间物流的映射关系
        :return:
        """
        sql = 'select UKLE, UKMA from ur_clear_ebay_adjust_express_store_express_mapping'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        out = dict()
        for row in ret:
            out[row['UKLE']] = row['UKMA']
        return out

    def close(self):
        self.base_dao.close_cur(self.cur)

    def pre_handle(self, order_time):
        """
        1. 把利润大于50RMB的订单的物流直接改为[Hermes - Standard 48 Claim(2-3 working days Service)-UKLE]
        1.计算物流方式的比率和改物流方式之前，先把偏远地区的 [Hermes - UK Standard 48 (Economy 2-3 working days Service)-UKLE]
        改为 [Royal Mail - Tracked 48 Parcel]
        :return:
        """
        sql = 'exec ur_clear_ebay_adjust_express_to_change_order_pre @orderTime=%s'

        all_store_specail_express = [
            'UKLE-Hermes - UK Standard 48',
            self.express_mapping['UKLE-Hermes - UK Standard 48'],
        ]
        self.cur.execute(sql, (order_time,))
        ret = self.cur.fetchall()
        for row in ret:

            for code in special_post_codes:
                # 偏远地区
                if re.sub(r'\s', '', str.upper(row['shipToZip'])).startswith(code):
                    row['remote'] = 1
                    break

            if row.get('remote'):
                # 利润大于50
                if row['ProfitMoney'] > self.profit_money:
                        row['newName'] = 'UKLE-Royal Mail - Tracked 48 Parcel'

                # 特殊的物流
                if row['name'] in all_store_specail_express:
                    row['newName'] = 'UKLE-Royal Mail - Tracked 48 Parcel'

            else:
                # 利润大于50
                if row['ProfitMoney'] > self.profit_money:
                    row['newName'] = 'UKLE-Hermes - Standard 48 Claim'

                # 改过物流的订单才执行
            if row.get('newName'):
                self.change_express_transaction(row)

    def get_low_rate_suffix(self, order_time):
        # 获取不达标的账号
        sql = 'exec ur_clear_ebay_adjust_express_suffix_rate @orderTime=%s'
        self.cur.execute(sql, (order_time,))
        ret = self.cur.fetchall()
        for row in ret:
            if row['rate'] < self.rate:
                row['number_to_change'] = math.ceil((self.rate - float(row['rate'])) * row['allOrderNumber'])
                yield row

    def get_all_orders(self, order_time):
        # 获取所有可以被更改的订单,不包含本身已经是挂号的订单
        sql = 'exec ur_clear_ebay_adjust_express_to_change_order_not_tracked @orderTime=%s'
        self.cur.execute(sql, (order_time,))
        ret = self.cur.fetchall()
        out = dict()
        for row in ret:
            if row['suffix'] not in out:
                out[row['suffix']] = [row]
            else:
                out[row['suffix']].append(row)
        self.all_orders = out

    def get_to_change_order(self, suffix_info):
        # 获取需要被更改的订单
        suffix_name = suffix_info['suffix']
        need_order_number = suffix_info['number_to_change']
        suffix_orders = self.all_orders.get(suffix_name, [])
        if need_order_number >= len(suffix_orders):
            return suffix_orders
        else:
            return suffix_orders[:need_order_number]

    def get_order_new_express(self, order_time):
        # 找到需要被更改的订单，并根据邮编匹配最佳物流
        self.get_all_orders(order_time)
        all_suffix = self.get_low_rate_suffix(order_time)
        out = list()
        for sf in all_suffix:
            to_change_orders = self.get_to_change_order(sf)
            for od in to_change_orders:
                for code in special_post_codes:
                    if re.sub(r'\s', '', str.upper(od['shipToZip'])).startswith(code):
                        od['newName'] = 'UKLE-Royal Mail - Tracked 48 Parcel'
                        od['suffixChangNumber'] = sf['number_to_change']
                        out.append(od)
                        break
                else:
                    od['suffixChangNumber'] = sf['number_to_change']
                    od['newName'] = 'UKLE-Hermes - UK Standard 48'
                out.append(od)
        return out

    def test_get_order_new_express(self):
        # 找到需要被更改的订单，并根据邮编匹配最佳物流
        out = list()
        all_suffix = [{"number_to_change": 40}]
        to_change_orders = [{"shipToZip": "ph3    1JK"}]

        for sf in all_suffix:
            for od in to_change_orders:
                for code in special_post_codes:
                    if re.sub(r'\s', '', str.upper(od['shipToZip'])).startswith(code):
                        od['newName'] = 'UKLE-Royal Mail - Tracked 48 Parcel'
                        od['suffixChangNumber'] = sf['number_to_change']
                        out.append(od)
                        break
                else:
                    od['suffixChangNumber'] = sf['number_to_change']
                    od['newName'] = 'UKLE-Hermes - UK Standard 48'
                    out.append(od)
        return out

    def set_order_new_express(self, order):
        # 设置订单新的物流方式,经过派单处理之后，在非E邮宝或者缺货状态中，申请跟踪号
        # logicsWayNID

        try:
            sql = f'update p_trade set logicsWayNid = {self.all_logistics_ways[order["newName"]]} where nid = {order["nid"]}'
            self.cur.execute(sql)
            self.logger.info(f'success to update {order["nid"]} of {order["suffix"]} set express to {order["newName"]}')
        except Exception as why:
            self.logger.info(f'fail to update {order["nid"]} of {order["suffix"]} cause of {why}')
            raise Exception(f'fail to set {order["nid"]} to {self.all_logistics_ways[order["newName"]]} ')

    def calculate_express_fee(self, order):
        # 重新计算物流费用
        try:
            calculate_sql = f'exec P_Fr_CalcShippingCostByNid {order["nid"]}'
            log_sql = f"exec S_WriteTradeLogs  '{order['nid']}', '运费计算', 'ur_cleaner'"
            self.cur.execute(calculate_sql)
            self.cur.execute(log_sql)
        except Exception as why:
            self.logger.info(f'fail to update {order["nid"]} of {order["suffix"]} cause of {why}')
            raise Exception(f'fail to calculate express fee of {order["nid"]}')

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

    def replace_express(self, order):
        """
        根据仓库选择物流
        :param order:
        :return:
        """

        # 万邑通UK仓
        if order['storeId'] == 26:
            pass

        # 万邑通MA仓
        if order['storeId'] == 54:
            order['newName'] = self.express_mapping[order['newName']]
        return order

    def change_express_transaction(self, order):
        """"
        更改物流的事务
        """
        try:

            # 根据仓库，选择对应的物流

            order = self.replace_express(order)

            # 改物流信息
            self.set_order_new_express(order)

            # 计算运费
            self.calculate_express_fee(order)

            # 加日志
            self.set_log(order)

            # 提交
            self.con.commit()
        except Exception as why:
            self.logger.error(f'fail to change express  {order["nid"]} cause of {why}')

    def trans(self, order_time):

        # 先修改偏远地区物流
        self.pre_handle(order_time)

        # # 再计算物流比率，修改物流方式
        # orders = self.get_order_new_express(order_time)
        # for od in orders:
        #     self.change_express_transaction(od)

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






