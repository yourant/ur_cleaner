import datetime
import os
import time
from src.services.base_service import CommonService
from src.services import oauth_wyt as wytOauth
import json
import requests


class CreateWytOutBoundOrder(CommonService):

    def __init__(self):
        super().__init__()
        self.base_url = "http://openapi.winit.com.cn/openapi/service"
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    def check_order_current_status(self, order):
        # 检查当前状态
        sql = (f"select nid,isnull(TrackNo,'') as TrackNo from p_trade(nolock) where nid = {order['NID']} "
               f" and FilterFlag = 6 and isnull(TrackNo,'')='' ")
        self.cur.execute(sql)
        ret = self.cur.fetchone()
        if ret:
            return True
        return False

    def create_wyt_order(self, data):
        # 创建万邑通出库单
        action = 'createOutboundOrder'
        try:
            oauth = wytOauth.Wyt()
            params = oauth.get_request_par(data, action)
            res = requests.post(self.base_url, json=params)
            ret = json.loads(res.content)
            trackingNum = ''
            outboundOrderNum = ''
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
                'orderNum': outboundOrderNum,
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
        sql = 'UPDATE p_trade SET TrackNo=%s,BUILD=%s WHERE NID=%s'
        out_stock_sql = 'UPDATE P_TradeUn SET TrackNo=%s WHERE NID=%s'
        log_sql = 'INSERT INTO P_TradeLogs(TradeNID,Operator,Logs) VALUES (%s,%s,%s)'
        try:
            if data['trackingNum']:
                self.cur.execute(sql, (data['trackingNum'], data['orderNum'], data['order_id']))
                self.cur.execute(out_stock_sql, (data['trackingNum'], data['order_id']))
            self.cur.execute(log_sql, (data['order_id'], 'ur_cleaner', data['Logs']))
            self.con.commit()
        except Exception as why:
            self.logger.error(
                f"failed to modify tracking number of order No. {data['order_id']} cause of {why} ")

    def get_order_data(self):
        # 万邑通仓库 派至非E邮宝 订单  和 万邑通仓库 缺货订单
        sql = ("SELECT  bw.serviceCode,t.* FROM "
               "(SELECT * FROM [dbo].[p_trade](nolock) WHERE FilterFlag = 6 AND expressNid = 5 AND isnull(trackno,'') = ''  and datediff(month,orderTime,getDate()) <= 1"
               " UNION SELECT * FROM [dbo].[P_TradeUn](nolock) WHERE FilterFlag = 1 AND expressNid = 5 AND isnull(trackno,'') = '' and datediff(month,orderTime,getDate()) <= 1 ) t "
               "LEFT JOIN B_LogisticWay(nolock) bw ON t.logicsWayNID=bw.NID "
               " where suffix in (select suffix from ur_clear_ebay_adjust_express_accounts)"
               # "WHERE suffix IN ('eBay-C99-tianru98','eBay-C100-lnt995','eBay-C142-polo1_13','eBay-C25-sunnyday0329','eBay-C127-qiju_58','eBay-C136-baoch-6338') "
               " -- and t.NID=22351335 ")

        self.cur.execute(sql)
        rows = self.cur.fetchall()
        for row in rows:
            yield row

    def _parse_order_data(self, order):
        # 整理数据格式

        current_order_info_sql = ("SELECT  bw.serviceCode,t.* FROM "
               "(SELECT * FROM [dbo].[p_trade](nolock) WHERE FilterFlag = 6 AND expressNid = 5 AND isnull(trackno,'') = ''  and datediff(month,orderTime,getDate()) <= 1"
               " UNION SELECT * FROM [dbo].[P_TradeUn](nolock) WHERE FilterFlag = 1 AND expressNid = 5 AND isnull(trackno,'') = '' and datediff(month,orderTime,getDate()) <= 1 ) t "
               "LEFT JOIN B_LogisticWay(nolock) bw ON t.logicsWayNID=bw.NID "
               " where t.nid = %s and suffix in (select suffix from ur_clear_ebay_adjust_express_accounts) "
                                   )

        self.cur.execute(current_order_info_sql, order['NID'])
        new_order = self.cur.fetchone()
        if not new_order:
            return

        order = new_order
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
        detail_sql = "SELECT * FROM p_tradeDt(nolock) WHERE tradeNid=%s UNION SELECT * FROM p_tradeDtUn(nolock) WHERE tradeNid=%s"
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
                if data and self.check_order_current_status(order):
                    print(data)
                    self.create_wyt_order(data)
        except Exception as e:
            self.logger.error(e)
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()
        print('程序耗时{:.2f}'.format(time.time() - begin_time))  # 计算程序总耗时


# 执行程序
if __name__ == "__main__":
    worker = CreateWytOutBoundOrder()
    worker.run()
