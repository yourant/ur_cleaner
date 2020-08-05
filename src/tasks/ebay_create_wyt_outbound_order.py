import datetime
import time
from src.services.base_service import BaseService
from src.services import oauth_wyt as wytOauth
import json
import requests


class CreateWytOutBoundOrder(BaseService):
    def __init__(self):
        # 初始化
        super().__init__()
        self.base_url = "http://openapi.winit.com.cn/openapi/service"

    def create_wyt_order(self, data):
        # 创建万邑通出库单
        action = 'createOutboundOrder'
        try:
            oauth = wytOauth.Wyt()
            params = oauth.get_request_par(data, action)
            res = requests.post(self.base_url, json=params)
            ret = json.loads(res.content)
            trackingNum = ''
            if ret['code'] == 0:
                outboundOrderNum = ret['data']['outboundOrderNum']
                # outboundOrderNum = 'WO3383663327'
                trackingNum = self.get_package_number(outboundOrderNum)
                if len(trackingNum) == 0:
                    trackingNum = '待取跟踪号'
                    logs = ('ur_cleaner ' + str(datetime.datetime.today())[:19] + ' >订单编号:' + str(
                        data['sellerOrderNo']) +
                            ' 提交订单成功! 跟踪号: 待取跟踪号  内部单号:' + str(outboundOrderNum))
                else:
                    logs = ('ur_cleaner ' + str(datetime.datetime.today())[:19] + ' >订单编号:' + str(
                        data['sellerOrderNo']) +
                            ' 获取跟踪号成功! 跟踪号:' + str(trackingNum))
            else:
                logs = ('ur_cleaner ' + str(datetime.datetime.today())[:19] + ' >订单编号:' + str(data['sellerOrderNo']) +
                        ' 提交订单失败! 跟踪号:  错误信息:' + str(ret['msg']))
                # 异常处理
                # 加其它备注:邮编,地址超长,收货人,尺寸
                self.update_order_remark(data['sellerOrderNo'], ret['msg'])

            update_params = {
                'trackingNum': trackingNum,
                'order_id': data['sellerOrderNo'],
                'Logs': logs
            }
            self.update_order(update_params)
            self.logger.info(f'success to create {data["sellerOrderNo"]}')

        except Exception as e:
            self.logger.error(f'failed to create {data["sellerOrderNo"]} cause of {e}')

    def get_package_number(self, order_num):
        # 获取跟踪号
        action = 'queryOutboundOrder'
        data = {
            'outboundOrderNum': order_num
        }
        oauth = wytOauth.Wyt()
        params = oauth.get_request_par(data, action)
        trackingNum = ''
        res = requests.post(self.base_url, json=params)
        ret = json.loads(res.content)
        if ret['code'] == 0:
            try:
                trackingNum = ret['data']['list'][0]['trackingNum']
            except BaseException:
                trackingNum = ''
        return trackingNum

    def update_order_remark(self, order_id, content):
        # 标记失败状态
        sql = ("if not EXISTS (select TradeNID from CG_OutofStock_Total(nolock) where TradeNID=%s) "
               " insert into CG_OutofStock_Total(TradeNID,PrintMemoTotal) values(%s,%s)"
               "else update CG_OutofStock_Total set PrintMemoTotal=%s where TradeNID=%s")
        try:
            self.cur.execute(
                sql, (order_id, order_id, content, content, order_id))
            self.con.commit()
        except Exception as why:
            self.logger.error(
                f"failed to modify PrintMemoTotal of order No. {order_id} cause of {why} ")

    def update_order(self, data):
        # 更新订单跟踪号
        sql = 'UPDATE p_trade SET TrackNo=%s WHERE NID=%s'
        out_stock_sql = 'UPDATE P_TradeUn SET TrackNo=%s WHERE NID=%s'
        log_sql = 'INSERT INTO P_TradeLogs(TradeNID,Operator,Logs) VALUES (%s,%s,%s)'
        try:
            self.cur.execute(sql, (data['trackingNum'], data['order_id']))
            self.cur.execute(
                out_stock_sql,
                (data['trackingNum'],
                 data['order_id']))
            self.cur.execute(
                log_sql, (data['order_id'], 'ur_cleaner', data['Logs']))
            self.con.commit()
        except Exception as why:
            self.logger.error(
                f"failed to modify tracking number of order No. {data['order_id']} cause of {why} ")

    def get_order_data(self):
        # 万邑通仓库 派至非E邮宝 订单  和 万邑通仓库 缺货订单
        sql = ("SELECT  bw.serviceCode,t.* FROM "
               "(SELECT * FROM [dbo].[p_trade](nolock) WHERE FilterFlag = 6 AND expressNid = 5 AND isnull(trackno,'') = ''  and datediff(month,orderTime,getDate()) <= 1"
               " UNION SELECT * FROM [dbo].[P_TradeUn](nolock) WHERE FilterFlag = 1 AND expressNid = 5 AND isnull(trackno,'') = '' and datediff(month,orderTime,getDate()) <= 1 ) t "
               "LEFT JOIN B_LogisticWay bw ON t.logicsWayNID=bw.NID "
               " where suffix in ('eBay-C127-qiju_58', 'eBay-C128-qiju80', 'eBay-C129-njh-7385', 'eBay-C130-kjdw32', 'eBay-C33-graduateha7', 'eBay-C77-henghua_99', 'eBay-C78-hh2-61', 'eBay-C79-jlh-79', 'eBay-C80-hhsm-99', 'eBay-C84-powj20', 'eBay-C131-feng-4682', 'eBay-C132-hljf-26', 'eBay-C133-pol-6836', 'eBay-C134-mnj_93', 'eBay-C25-sunnyday0329', 'eBay-C85-hongse-6', 'eBay-C88-yt2343', 'eBay-C135-chang_8398', 'eBay-C136-baoch-6338', 'eBay-C137-bcsha-14', 'eBay-C138-clbc83', 'eBay-C45-realizeoh1', 'eBay-C46-displaywo2', 'eBay-C48-passengerwa4', 'eBay-C49-traditionaloh5', 'eBay-C64-bridgeha2', 'eBay-C65-concertwo3', 'eBay-C66-dictionaryye4', 'eBay-C67-expressionhe5', 'eBay-C99-tianru98', 'eBay-C100-lnt995', 'eBay-C101-trw-54', 'eBay-C123-anjua_803', 'eBay-C124-dalian5821', 'eBay-C95-shi_7040', 'eBay-C96-sysy_3270', 'eBay-B11-mainlandye1', 'eBay-B12-preventhe2', 'eBay-C57-captainha1', 'eBay-C59-restaurantwo3', 'eBay-B9-butterflywo1', 'eBay-B10-supposehe2', 'eBay-C149-ejy_94', 'eBay-C150-ygv_80', 'eBay-C151-kqq_37', 'eBay-C153-jcy368', 'eBay-C152-qage-77', 'eBay-C28-snowyday0329', 'eBay-73-outsidehe1', 'eBay-C139-jui384', 'eBay-C140-dlguy66', 'eBay-C141-aishan-42', 'eBay-C142-polo1_13')"
               # "WHERE suffix IN ('eBay-C99-tianru98','eBay-C100-lnt995','eBay-C142-polo1_13','eBay-C25-sunnyday0329','eBay-C127-qiju_58','eBay-C136-baoch-6338') "
               "-- AND t.NID=21383397 ")

        self.cur.execute(sql)
        rows = self.cur.fetchall()
        for row in rows:
            yield row

    def _parse_order_data(self, order):
        # 整理数据格式
        data = {
            "doorplateNumbers": "0",
            "address1": order["SHIPTOSTREET"],
            "address2": order["SHIPTOSTREET2"],
            "city": order["SHIPTOCITY"],
            "deliveryWayID": order["serviceCode"],
            "eBayOrderID": order["ACK"],
            "emailAddress": order["EMAIL"],
            "phoneNum": order["SHIPTOPHONENUM"],
            "recipientName": order["SHIPTONAME"],
            "region": order["SHIPTOSTATE"],
            "repeatable": "N",
            "sellerOrderNo": order["NID"],
            "state": order["SHIPTOCOUNTRYCODE"],
            "warehouseID": "1000069",
            "zipCode": order["SHIPTOZIP"]
        }
        detail_sql = "SELECT * FROM p_tradeDt WHERE tradeNid=%s UNION SELECT * FROM p_tradeDtUn WHERE tradeNid=%s"
        self.cur.execute(detail_sql, (order["NID"], order["NID"]))
        detail = self.cur.fetchall()
        productList = []
        for val in detail:
            item = {
                "eBayBuyerID": order["BUYERID"],
                "eBayItemID": val["L_NUMBER"],
                "eBaySellerID": order["User"],
                "eBayTransactionID": order['TRANSACTIONID'],
                "productCode": val["SKU"],
                "productNum": int(val["L_QTY"])
            }
            productList.append(item)
        data['productList'] = productList
        return data

    def run(self):
        begin_time = time.time()
        try:
            rows = self.get_order_data()
            for order in rows:
                data = self._parse_order_data(order)
                self.create_wyt_order(data)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.close()
        print('程序耗时{:.2f}'.format(time.time() - begin_time))  # 计算程序总耗时


# 执行程序
if __name__ == "__main__":
    worker = CreateWytOutBoundOrder()
    worker.run()
