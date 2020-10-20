#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-20 15:56
# Author: turpure


import os
import json
import requests
from tenacity import retry, stop_after_attempt
from src.services.base_service import CommonService
from src.services import oauth as aliOauth
import re
import datetime
import pandas as pd


class Worker(CommonService):
    """
    check purchased orders
    """
    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    # @retry(stop=stop_after_attempt(3))
    def get_order_details(self, order_info):
        order_id = order_info['orderId']
        oauth = aliOauth.Ali(order_info['account'])
        base_url = oauth.get_request_url(order_id)
        out = dict()
        try:
            res = requests.get(base_url)
            response = json.loads(res.content)
            try:
                if response['success'] == 'true':
                    ret = response['result']
                    total_money = self.get_total_money(order_info['billNumber'])
                    products = ret['productItems']
                    total_quantity = sum([ele['quantity'] for ele in products])
                    # avg_shipping_fee = ret['baseInfo']['shippingFee'] / total_quantity
                    avg_delta = (float(ret['baseInfo']['totalAmount']) - float(total_money)) / total_quantity

                    for pd in products:
                        if pd['specId'] == order_info['specId']:
                            out['newPrice'] = float(order_info['costprice'])
                            out['newTaxPrice'] = out['newPrice'] + avg_delta
                            out['newAllMoney'] = out['newTaxPrice'] * float(order_info['amount'])
                            out['newMoney'] = float(order_info['costprice']) * float(order_info['amount'])
                            out['nid'] = order_info['nid']
                            return out

                        # 计算价格

                else:
                    self.logger.error('error while get order details %s' % response['errorMessage'])
            except BaseException as why:
                self.logger.error(f'error while get order details  casuse of {why}' % why)
        except Exception as e:
            self.logger.error('error while get order details %s' % e)

    def get_order_info(self, order_info):
        order_id = order_info['orderId']
        oauth = aliOauth.Ali(order_info['account'])
        base_url = oauth.get_request_url(order_id)
        rows = []
        try:
            res = requests.get(base_url)
            response = json.loads(res.content)
            try:
                if response['success'] == 'true':
                    ret = response['result']
                    products = ret['productItems']
                    for pd in products:
                        out = {}
                        out['alibabaOrderId'] = order_info['orderId'] + '-'
                        try:
                            out['color'] = pd['skuInfos'][0]['value']
                        except Exception as why:
                            out['color'] = ''
                        try:
                            out['size'] = pd['skuInfos'][1]['value']
                        except Exception as why:
                            out['size'] = ''
                        out['specId'] = pd['specId']
                        rows.append(out)

            except BaseException as why:
                self.logger.error(f'error while get order details  casuse of {why}' % why)
        except Exception as e:
            self.logger.error('error while get order details %s' % e)
        return rows

    def update(self, order):
        try:
            sql = f"update cg_stockOrderD_price set worker='ur_cleaner1', newAllMoney={order['newAllMoney']}, newMoney={order['newMoney']}, newPrice={order['newPrice']},newTaxPrice={order['newTaxPrice']} where nid = {order['nid']} "
            self.cur.execute(sql)
            self.logger.info(f'update {order["nid"]} ')
            self.con.commit()
        except Exception as why:
            self.logger.error(f'failed to update {order} cause of {why}')

    def get_order_from_py(self):

        # 只有一个商品的采购单
        query = "select 'caigoueasy' as account,amount, nid,alibabaOrderid as orderId,isnull(costprice,price) as costprice from cg_stockOrderD_price where billNUmber in (select billNUmber from cg_stockOrderD_price   group by billNUmber   having count(billNUmber) =1) and  offerid is null and nid is not null and isnull(alibabaOrderid,'') != ''"

        # 只有一个商品的采购单
        query = "select 'caigoueasy' as account,cgm.orderMoney, amount, cp.nid,alibabaOrderid as orderId,isnull(costprice,price) as costprice from cg_stockOrderD_price where billNUmber in (select billNUmber from cg_stockOrderD_price   group by billNUmber   having count(billNUmber) > 1) and  offerid is null and nid is not null and isnull(alibabaOrderid,'') != '' and billNumber= 'CGD-2020-10-10-2972' "
        # 指定采购单
        # query = "select billNumber,'caigoueasy' as account,specId, amount, nid,alibabaOrderid as orderId,isnull(price,costprice) as costprice from cg_stockOrderD_price  where billNumber in (select billNumber from (select DISTINCT nid, billNumber from cg_stockORderd_price ) as cp   group by cp.billNumber having count(*) =1 )"
        query = "select billNumber,'caigoueasy' as account,specId, amount, nid,alibabaOrderid as orderId,isnull(price,costprice) as costprice from cg_stockOrderD_price where isnull(worker,'') !='ur_cleaner'  "
        # query = "select billNumber,'caigoueasy' as account,specId, amount, nid,alibabaOrderid as orderId,isnull(price,costprice) as costprice from cg_stockOrderD_price where billNumber='CGD-2020-10-08-4166'  "

        query = "select billNumber,'caigoueasy' as account,specId, amount, nid,alibabaOrderid as orderId,isnull(price,costprice) as costprice from cg_stockOrderD_price where billNumber='CGD-2020-10-06-0024'"


        # 绑定1688SKu 但是没有匹配到价格的SKU
        # query = "select top 100 cgm.orderMoney,'caigoueasy' as account,specId, amount, cp.nid,cp.alibabaOrderid as orderId,isnull(price,costprice) as costprice  from cg_stockOrderD_price as cp LEFT JOIN cg_stockOrderM as cgm on cp.billNumber = cgm.billNUmber where specId is not null and offerid is not  null "
        self.cur.execute(query)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_total_money(self, bill_number):
        """
        获取当前订单总额
        :param bill_number:
        :return:
        """
        sql = f"select sum(cp.price * cp.amount) as totalMoney from (select distinct  nid, price, amount,billNumber from cg_stockorderD_price where billNumber = '{bill_number}') as cp   group by cp.billNumber"
        self.cur.execute(sql)
        ret = self.cur.fetchone()
        return ret['totalMoney']

    def get_spec_task(self):
        orders = self.get_order_from_py()
        out = []
        for od in orders:
            rows = self.get_order_info(od)
            out.extend(rows)
        data = pd.DataFrame(out)
        data.sort_index(inplace=True)
        data.drop_duplicates(inplace=True)
        data.to_csv('orderSpec.csv', encoding='utf_8_sig')

    def get_price_task(self):
        orders = self.get_order_from_py()
        for od in orders:
            rows = self.get_order_details(od)
            self.update(rows)

    def work(self):
        try:
           self.get_price_task()
        except Exception as e:
            self.logger.error(e)
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()
