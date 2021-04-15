#! usr/bin/env/python3
# coding:utf-8
# Author: turpure

import os
from src.services.base_service import CommonService
from src.tasks.fetch_userSuffixMap import UserSuffixMapFetcher

class CalCleanPlanMainSeller(CommonService):
    """
    計算清倉產品的主責任人
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

    @staticmethod
    def get_new_seller_suffix_map():
        fetcher = UserSuffixMapFetcher()
        fetcher.work()

    def get_clean_product(self):
        sql = 'select goodsCode from  oauth_clearPlan where isRemoved=0'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row['goodsCode']

    def cal_seller(self, goods_code):
        sql = 'oauth_goodsCodeSuffixSold %s'
        self.cur.execute(sql, (goods_code,))
        ret = self.cur.fetchone()
        if not ret:
            return 'all'
        if ret['stockNumber'] >= 20:
            return 'all'
        if ret['username']:
            return ret['username']
        else:
            return 'all'

    def update_seller(self, sellers, goods_code):
        try:
            sql = 'update  oauth_clearPlan set sellers=%s where goodsCode=%s'
            self.cur.execute(sql, (sellers, goods_code))
            self.con.commit()
            self.logger.info(f'success to set {goods_code} main sellers to {sellers}')
        except Exception as why:
            self.logger.error(f'error to set {goods_code} main sellers to {sellers}')

    def trans(self):
        self.get_new_seller_suffix_map()
        products = self.get_clean_product()
        for pd in products:
            seller = self.cal_seller(pd)
            self.update_seller(seller, pd)

    def work(self):
        try:
            self.trans()
        except Exception as why:
            self.logger.error('fail to cal main seller map cause of {}'.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to cal main seller  {name}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = CalCleanPlanMainSeller()
    worker.work()
