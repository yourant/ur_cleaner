import datetime
import time
from configs.config import Config
from src.services import oauth_wyt as wytOauth
from src.tasks.ebay_change_express_remote import Shipper
import requests
import json
import os


class FetchEbayOrderPackageNumber(Shipper):
    def __init__(self):
        super().__init__()
        self.base_url = "http://openapi.winit.com.cn/openapi/service"
        self.config = Config().get_config('ebay.yaml')

    def get_order_data(self):
        # 万邑通仓库 派至非E邮宝 订单  和 万邑通仓库 缺货订单
        sql = ("SELECT * FROM [dbo].[p_trade](nolock) WHERE FilterFlag = 6 AND expressNid = 5 AND trackno ='待取跟踪号'  and datediff(month,orderTime,getDate()) <= 1 "
               " and suffix in (select suffix from ur_clear_ebay_adjust_express_accounts)"
               " union "
               "SELECT * FROM [dbo].[p_tradeun](nolock) WHERE FilterFlag = 1 AND expressNid = 5 AND trackno ='待取跟踪号'  and datediff(month,orderTime,getDate()) <= 1 "
               " and suffix in (select suffix from ur_clear_ebay_adjust_express_accounts)"
               )
        self.cur.execute(sql)
        rows = self.cur.fetchall()
        for row in rows:
            yield row

    def get_package_number(self, order):
        action = 'queryOutboundOrderList'
        begin = str(order['ORDERTIME'])[:10]
        end = str(datetime.datetime.today())[:10]
        data = {
            'sellerOrderNo': order['NID'],
            'dateOrderedStartDate': begin,
            'dateOrderedEndDate': end,
            'pageSize': 10,
            'pageNum': 1
        }
        oauth = wytOauth.Wyt()
        params = oauth.get_request_par(data, action)
        tracking_number = ''

        res = requests.post(self.base_url, json=params)
        ret = json.loads(res.content)
        if ret['code'] == 0:
            tracking_number = ret['data']['list'][0]['trackingNo']
        return tracking_number

    def update_order(self, data):
        sql = 'update p_trade set TrackNo=%s where NID=%s'
        out_stock_sql = 'update P_TradeUn set TrackNo=%s where NID=%s'
        log_sql = 'insert into P_TradeLogs(TradeNID,Operator,Logs) values(%s,%s,%s)'
        try:
            self.cur.execute(sql, (data['tracking_number'], data['order_id']))
            self.cur.execute(out_stock_sql, (data['tracking_number'], data['order_id']))
            self.cur.execute(log_sql, (data['order_id'], 'ur_cleaner', data['Logs']))
            self.con.commit()
            self.logger.info(data['Logs'])
        except Exception as why:
            self.con.rollback()
            self.logger.error(
                f"failed to modify tracking number of order No. {data['order_id']} cause of {why} ")

    def get_data_by_id(self, order):
        try:
            tracking_number = self.get_package_number(order)
            if tracking_number:
                logs = ('ur_cleaner ' + str(datetime.datetime.today())[:19] + ' >订单编号:' + str(order['NID']) +
                        ' 获取跟踪号成功! 跟踪号:' + str(tracking_number))
                update_params = {
                    'tracking_number': tracking_number,
                    'order_id': order['NID'],
                    'Logs': logs
                }
                # 修改跟踪号，添加操作日志
                self.update_order(update_params)

                # 标记平台发货
                # self.ship(order['NID'])
            else:
                self.logger.info(
                    'tracking no of order {} is empty!'.format(
                        order['NID']))
        except Exception as e:
            self.logger.error(
                'failed to get tracking no of order {} cause of {}'.format(order['NID'], e))

    def run(self):
        begin_time = time.time()
        try:
            rows = self.get_order_data()
            for rw in rows:
                self.get_data_by_id(rw)
        except Exception as e:
            self.logger.error(e)
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()
        print('程序耗时{:.2f}'.format(time.time() - begin_time))  # 计算程序总耗时


# 执行程序
if __name__ == "__main__":
    worker = FetchEbayOrderPackageNumber()
    worker.run()
