#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-07-21 13:14
# Author: turpure

from src.services.base_service import BaseService
import math
from src.tasks.ebay_change_express_config import special_post_codes
import datetime


class Updater(BaseService):
    def __init__(self):
        super().__init__()
        self.all_orders = dict()

    def get_low_rate_suffix(self, order_time):
        # 获取不达标的账号
        sql = 'exec ur_clear_ebay_adjust_express_suffix_rate @orderTime=%s'
        self.cur.execute(sql, (order_time,))
        ret = self.cur.fetchall()
        for row in ret:
            if row['rate'] < 0.25:
                row['number_to_change'] = math.ceil((0.22 - float(row['rate'])) * row['allOrderNumber'])
                yield row

    def get_all_orders(self, order_time):
        # 获取所有可以被更改的订单
        sql = 'exec ur_clear_ebay_adjust_express_to_change_order @orderTime=%s'
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
                    if od['shipToZip'].startswith(code):
                        od['newName'] = 'Royal Mail - Tracked 48 Parcel'
                        od['suffixChangNumber'] = sf['number_to_change']
                        out.append(od)
                        break
                else:
                    od['suffixChangNumber'] = sf['number_to_change']
                    od['newName'] = 'Hermes - UK Standard 48 (Economy 2-3 working days Service)-UKLE'
                    out.append(od)

        return out

    def set_order_new_express(self, order):
        # 设置订单新的物流方式,经过派单处理之后，在非E邮宝或者缺货状态中，申请跟踪号
        # logicsWayNID
        logistics_ways = {
            'Hermes - UK Standard 48 (Economy 2-3 working days Service)-UKLE': 524,
            'Royal Mail - Tracked 48 Parcel': 283
        }

        sql = f'update p_trade set logicsWayNid = {logistics_ways[order["newName"]]} where nid = {order["nid"]}'
        try:
            self.cur.execute(sql)
            self.set_log(order)
            self.logger.info(f'success to update {order["nid"]} of {order["suffix"]} set express to {order["newName"]}')
        except Exception as why:
            self.logger.info(f'fail to update {order["nid"]} of {order["suffix"]} cause of {why}')

    def set_log(self, order):
        """
        记录操作日志
        :param order:
        :return:
        """
        sql = 'INSERT INTO P_TradeLogs(TradeNID,Operator,Logs) VALUES (%s,%s,%s)'
        try:
            logs = ('ur_cleaner ' + str(datetime.datetime.today())[:19] + ' 更改物流方式为 ' + order['newName'])
            self.cur.execute(sql, (order['nid'], 'ur_cleaner', logs))
            self.con.commit()
            # self.logger.info(f'success to set log of {order["nid"]}')
        except Exception as why:
            self.logger.error(f'fail to set log of {order["nid"]}')

    def trans(self, order_time):
        orders = self.get_order_new_express(order_time)
        for od in orders:
            self.set_order_new_express(od)

    def work(self):
        try:
            today = str(datetime.datetime.now())[:10]
            yesterday = str(datetime.datetime.now() - datetime.timedelta(days=1))[:10]
            for day in [today, yesterday]:
                self.trans(day)

        except Exception as why:
            self.logger.error(f'fail to finish task cause of {why}')
        finally:
            pass


# 执行程序
if __name__ == "__main__":
    worker = Updater()
    worker.work()






