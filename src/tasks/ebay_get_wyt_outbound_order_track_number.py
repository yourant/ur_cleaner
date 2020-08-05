import datetime
import time
from src.services.base_service import BaseService
from configs.config import Config
from src.services import oauth_wyt as wytOauth
from src.tasks.ebay_change_express_remote import Shipper
from concurrent.futures import ThreadPoolExecutor as Pool
import requests
import json


class FetchEbayOrderPackageNumber(Shipper):
    def __init__(self):
        super().__init__()
        self.base_url = "http://openapi.winit.com.cn/openapi/service"
        self.config = Config().get_config('ebay.yaml')

    def get_order_data(self):
        # 万邑通仓库 派至非E邮宝 订单  和 万邑通仓库 缺货订单
        sql = ("SELECT * FROM [dbo].[p_trade](nolock) WHERE FilterFlag = 6 AND expressNid = 5 AND trackno ='待取跟踪号'  and datediff(month,orderTime,getDate()) <= 1 "
               " and suffix in ('eBay-C127-qiju_58', 'eBay-C128-qiju80', 'eBay-C129-njh-7385', 'eBay-C130-kjdw32', 'eBay-C33-graduateha7', 'eBay-C77-henghua_99', 'eBay-C78-hh2-61', 'eBay-C79-jlh-79', 'eBay-C80-hhsm-99', 'eBay-C84-powj20', 'eBay-C131-feng-4682', 'eBay-C132-hljf-26', 'eBay-C133-pol-6836', 'eBay-C134-mnj_93', 'eBay-C25-sunnyday0329', 'eBay-C85-hongse-6', 'eBay-C88-yt2343', 'eBay-C135-chang_8398', 'eBay-C136-baoch-6338', 'eBay-C137-bcsha-14', 'eBay-C138-clbc83', 'eBay-C45-realizeoh1', 'eBay-C46-displaywo2', 'eBay-C48-passengerwa4', 'eBay-C49-traditionaloh5', 'eBay-C64-bridgeha2', 'eBay-C65-concertwo3', 'eBay-C66-dictionaryye4', 'eBay-C67-expressionhe5', 'eBay-C99-tianru98', 'eBay-C100-lnt995', 'eBay-C101-trw-54', 'eBay-C123-anjua_803', 'eBay-C124-dalian5821', 'eBay-C95-shi_7040', 'eBay-C96-sysy_3270', 'eBay-B11-mainlandye1', 'eBay-B12-preventhe2', 'eBay-C57-captainha1', 'eBay-C59-restaurantwo3', 'eBay-B9-butterflywo1', 'eBay-B10-supposehe2', 'eBay-C149-ejy_94', 'eBay-C150-ygv_80', 'eBay-C151-kqq_37', 'eBay-C153-jcy368', 'eBay-C152-qage-77', 'eBay-C28-snowyday0329', 'eBay-73-outsidehe1', 'eBay-C139-jui384', 'eBay-C140-dlguy66', 'eBay-C141-aishan-42', 'eBay-C142-polo1_13')"
               " union "
               "SELECT * FROM [dbo].[p_tradeun](nolock) WHERE FilterFlag = 1 AND expressNid = 5 AND trackno ='待取跟踪号'  and datediff(month,orderTime,getDate()) <= 1 "
               " and suffix in ('eBay-C127-qiju_58', 'eBay-C128-qiju80', 'eBay-C129-njh-7385', 'eBay-C130-kjdw32', 'eBay-C33-graduateha7', 'eBay-C77-henghua_99', 'eBay-C78-hh2-61', 'eBay-C79-jlh-79', 'eBay-C80-hhsm-99', 'eBay-C84-powj20', 'eBay-C131-feng-4682', 'eBay-C132-hljf-26', 'eBay-C133-pol-6836', 'eBay-C134-mnj_93', 'eBay-C25-sunnyday0329', 'eBay-C85-hongse-6', 'eBay-C88-yt2343', 'eBay-C135-chang_8398', 'eBay-C136-baoch-6338', 'eBay-C137-bcsha-14', 'eBay-C138-clbc83', 'eBay-C45-realizeoh1', 'eBay-C46-displaywo2', 'eBay-C48-passengerwa4', 'eBay-C49-traditionaloh5', 'eBay-C64-bridgeha2', 'eBay-C65-concertwo3', 'eBay-C66-dictionaryye4', 'eBay-C67-expressionhe5', 'eBay-C99-tianru98', 'eBay-C100-lnt995', 'eBay-C101-trw-54', 'eBay-C123-anjua_803', 'eBay-C124-dalian5821', 'eBay-C95-shi_7040', 'eBay-C96-sysy_3270', 'eBay-B11-mainlandye1', 'eBay-B12-preventhe2', 'eBay-C57-captainha1', 'eBay-C59-restaurantwo3', 'eBay-B9-butterflywo1', 'eBay-B10-supposehe2', 'eBay-C149-ejy_94', 'eBay-C150-ygv_80', 'eBay-C151-kqq_37', 'eBay-C153-jcy368', 'eBay-C152-qage-77', 'eBay-C28-snowyday0329', 'eBay-73-outsidehe1', 'eBay-C139-jui384', 'eBay-C140-dlguy66', 'eBay-C141-aishan-42', 'eBay-C142-polo1_13')"
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
            self.cur.execute(
                out_stock_sql,
                (data['trackingNum'],
                 data['order_id']))
            self.cur.execute(
                log_sql, (data['order_id'], 'ur_cleaner', data['Logs']))
            self.con.commit()
            self.logger.info(data['Logs'])
        except Exception as why:
            self.logger.error(
                f"failed to modify tracking number of order No. {data['order_id']} cause of {why} ")

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
                self.ship(order['NID'])
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
