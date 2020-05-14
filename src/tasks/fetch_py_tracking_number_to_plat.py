from src.services.base_service import BaseService
from configs.config import Config
import requests
import json


class Marker(BaseService):

    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')


    def get_order_data(self):
        sql = ("SELECT DISTINCT  e.code,token,suffix,ack,trackNo,closingdate FROM P_Trade (nolock) m " +
            "LEFT JOIN P_TradeDt dt ON m.nid = dt.tradeNid LEFT JOIN T_express e ON e.nid = m.expressnid " +
            "LEFT JOIN (SELECT MAX(EbayTOKEN) AS token,NoteName FROM S_PalSyncInfo WHERE SyncEbayEnable=1 GROUP BY NoteName) p " +
            "ON m.suffix = p.NoteName WHERE m.FilterFlag = 100 AND m.shippingmethod = 0 " +
            "AND m.addressowner='ebay' AND dt.storeId=7 AND ISNULL(m.trackNo,'')<>'' " +
            "AND e.name NOT IN ('万邑通','SpeedPAK') " +
            "AND CONVERT(VARCHAR(10),m.CLOSINGDATE,121) BETWEEN  %s AND %s " )
        self.cur.execute(sql,('2020-05-01','2020-05-11'))
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def update_ebay_tracking_number(self, data):
        for item in data:
            headers = {'Content-Type':'application/json','Authorization': item['token']}
            #url = "https://api.ebay.com/sell/fulfillment/v1/order/" + item['ack'] + "/shipping_fulfillment"
            url = "https://api.ebay.com/sell/fulfillment/v1/order/358618/shipping_fulfillment"
            payload = {
                "lineItems" : [
                    {
                        "lineItemId": "143499140028",
                        "quantity": 1}
                ],
                "shippedDate": "2020-05-05 17:18",
                "shippingCarrierCode": "yanwen",
                "trackingNumber": "UC972504634YP"
            }
            print(item)

            ret = requests.post(url, data=json.dumps(payload), headers=headers).json()
            print(ret)



    def run(self):
        try:
            print(self.config)
            # data = self.get_order_data()
            # self.update_ebay_tracking_number(data)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.close()



if __name__ == '__main__':
    worker = Marker()
    worker.run()



