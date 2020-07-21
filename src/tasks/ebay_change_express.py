#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-07-21 13:14
# Author: turpure

from src.services.base_service import BaseService
from pymongo import MongoClient

mongo = MongoClient('192.168.0.150', 27017)
mongodb = mongo['ebay']
col = mongodb['ebay_product_list']


class Updater(BaseService):

    def get_low_rate_suffix(self):
        # 获取不达标的账号
        sql = 'exec ur_clear_ebay_adjust_express_suffix_rate'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            if row['rate'] < 0.22:
                yield row

    def get_to_change_order(self, suffix):
        sql = 'exec ur_clear_ebay_adjust_express_to_change_order'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            if row['suffix'] == suffix:
                yield row

    def compute_express_rate(self):
        # 计算物流比例
        pass

    def pick_order(self):
        # 挑选待修改订单
        pass

    def local_update(self):
        # 本地状态修改
        pass

    def handle_failed(self):
        # 失败处理
        pass

    def remote_upload(self):
        # 上传接口
        pass

    def trans(self):
        pass

    def work(self):
        try:
            suffix = self.get_to_change_order('eBay-C127-qiju_58')
            for sf in suffix:
                print(sf)
        except Exception as why:
            print(why)
        finally:
            pass


# 执行程序
if __name__ == "__main__":
    worker = Updater()
    worker.work()






