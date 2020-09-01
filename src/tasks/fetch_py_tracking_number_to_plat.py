from src.services.base_service import CommonService
from configs.config import Config
import datetime
from ebaysdk.trading import Connection as Trading
import os

class Marker(CommonService):

    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')
        self.batch_id = str(datetime.datetime.now() - datetime.timedelta(days=7))[:10]
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    def get_order_data(self):
        sql = ("SELECT DISTINCT m.nid,e.code,token,suffix,ack,trackNo,transactionid," 
               "closingdate + ':00' as closingdate,l_ebayitemtxnid,l_number,l_qty FROM P_Trade (nolock) m " 
            "LEFT JOIN P_TradeDt dt ON m.nid = dt.tradeNid LEFT JOIN T_express e ON e.nid = m.expressnid " 
            "LEFT JOIN (SELECT MAX(EbayTOKEN) AS token,NoteName FROM S_PalSyncInfo WHERE SyncEbayEnable=1 GROUP BY NoteName) p " 
            "ON m.suffix = p.NoteName WHERE m.FilterFlag = 100 AND m.shippingmethod = 0 " 
            "AND m.addressowner='ebay' AND dt.storeId=7 AND ISNULL(m.trackNo,'')<>'' " 
            "AND e.name NOT IN ('万邑通','SpeedPAK') " 
            "AND CONVERT(VARCHAR(10),m.CLOSINGDATE,121) BETWEEN  %s AND %s " 
               # " and suffix = '11-newfashion' "
               # " and m.nid = 19817483"
               )
        yesterday = str(datetime.datetime.today() - datetime.timedelta(days=1))[:10]
        # yesterday = '2020-04-01'
        last_day = str(datetime.datetime.strptime(yesterday[:10], '%Y-%m-%d') - datetime.timedelta(days=30))[:10]
        self.cur.execute(sql,(last_day, yesterday))
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def update_py_trade_status(self, trade_id):
        sql = "UPDATE P_Trade (nolock) SET shippingmethod = 1 WHERE nid=%s"
        self.cur.execute(sql, trade_id)
        self.con.commit()

    def update_ebay_tracking_number(self, data):
        for item in data:

            api = Trading(config_file=self.config)
            params = {
                    "RequesterCredentials": {"eBayAuthToken": item['token']},
                    'OrderLineItemID': item['l_number'] + '-' + item['l_ebayitemtxnid'],
                    'Shipment':{
                        'ShipmentTrackingDetails': {
                            "ShipmentLineItem":{
                                'LineItem':{
                                    'ItemID': item['l_number'],
                                    'Quantity': int(item['l_qty']),
                                    'TransactionID': item['l_ebayitemtxnid']
                                }
                            },
                            'ShipmentTrackingNumber': item['trackNo'],
                            # 'ShipmentTrackingNumber': '1231appy',
                            'ShippingCarrierUsed': item['code'],
                        },
                        'ShippedTime':item['closingdate']
                    },
                }

            try:
                response = api.execute('CompleteSale', params)
                result = response.dict()
                if result['Ack'] == 'Success':
                    self.update_py_trade_status(item['nid'])
                    self.logger.error('success to fetch tracking number of order num {}'.format(item['nid']))
                else:
                    self.logger.error('failed to fetch tracking number of order num {}'.format(item['nid']))
            except Exception as e:
                self.logger.error('failed to fetch tracking number of order num {} cause of {}'.format(item['nid'], e))

    def run(self):
        try:
            data = self.get_order_data()
            self.update_ebay_tracking_number(data)
        except Exception as e:
            self.logger.error(e)
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == '__main__':
    worker = Marker()
    worker.run()



