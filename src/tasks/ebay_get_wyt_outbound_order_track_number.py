import datetime
import time
from src.services.base_service import BaseService
from configs.config import Config
from src.services import oauth_wyt as wytOauth
from src.tasks import ebay_change_express_remote as ebayExpress
from concurrent.futures import ThreadPoolExecutor as Pool
import requests
import json


class FetchEbayOrderPackageNumber(BaseService):
    def __init__(self):
        super().__init__()
        self.base_url = "http://openapi.winit.com.cn/openapi/service"
        self.config = Config().get_config('ebay.yaml')

    def get_order_data(self):
        # 万邑通仓库 派至非E邮宝 订单  和 万邑通仓库 缺货订单
        sql = ("SELECT * FROM [dbo].[p_trade](nolock) WHERE FilterFlag = 6 AND expressNid = 5 AND trackno ='待取跟踪号'  and datediff(month,orderTime,getDate()) = 1 union "
               "SELECT * FROM [dbo].[p_tradeun](nolock) WHERE FilterFlag = 6 AND expressNid = 5 AND trackno ='待取跟踪号'  and datediff(month,orderTime,getDate()) = 1")
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
            'pageSize':10,
            'pageNum':1
        }
        oauth = wytOauth.Wyt()
        params = oauth.get_request_par(data, action)
        trackingNum = ''

        res = requests.post(self.base_url, json=params)
        ret = json.loads(res.content)
        if ret['code'] == 0:
            trackingNum = ret['data']['list'][0]['trackingNo']
            # print(trackingNum)
        return trackingNum

    def update_order(self, data):
        sql = 'update p_trade set TrackNo=%s where NID=%s'
        out_stock_sql = 'update P_TradeUn set TrackNo=%s where NID=%s'
        log_sql = 'insert into P_TradeLogs(TradeNID,Operator,Logs) values(%s,%s,%s)'
        try:
            self.cur.execute(sql, (data['trackingNum'], data['order_id']))
            self.cur.execute(out_stock_sql, (data['trackingNum'], data['order_id']))
            self.cur.execute(log_sql, (data['order_id'], 'ur_cleaner', data['Logs']))
            self.con.commit()
            self.logger.info(data['Logs'])
        except Exception as why:
            self.logger.error(f"failed to modify tracking number of order No. {data['order_id']} cause of {why} ")

    def get_data_by_id(self, order):
        try:
            trackingNum = self.get_package_number(order)
            if trackingNum:
                logs = ('ur_cleaner ' + str(datetime.datetime.today())[:19] + ' >订单编号:' + str(order['NID']) +
                        ' 获取跟踪号成功! 跟踪号:' + str(trackingNum))
                update_params = {
                    'trackingNum': trackingNum,
                    'order_id': order['NID'],
                    'Logs': logs
                }
                # 修改跟踪号，添加操作日志
                self.update_order(update_params)
                # 标记平台发货
                ebayExpress.Shipper(order['NID']).run()
            else:
                self.logger.info('tracking no of order {} is empty!'.format(order['NID']))
        except Exception as e:
            self.logger.error('failed to get tracking no cause of {}'.format(e))

    def run(self):
        begin_time = time.time()
        try:
            rows = self.get_order_data()
            for rw in rows:
                self.get_data_by_id(rw)
            # with Pool(8) as pl:
            #     pl.map(self.get_data_by_id, rows)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.close()
        print('程序耗时{:.2f}'.format(time.time() - begin_time))  # 计算程序总耗时


# 执行程序
if __name__ == "__main__":
    worker = FetchEbayOrderPackageNumber()
    worker.run()
