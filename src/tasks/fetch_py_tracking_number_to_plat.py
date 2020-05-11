from src.services.base_service import BaseService
from configs.config import Config

from ebaysdk.finding import Connection as Finding
from ebaysdk.exception import ConnectionError
from ebaysdk.trading import Connection as Trading



class Marker(BaseService):

    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')


    def get_order_data(self):
        sql = ("SELECT DISTINCT  suffix,ack,trackNo FROM P_Trade (nolock) m " +
            "LEFT JOIN P_TradeDt dt ON m.nid = dt.tradeNid " +
            "LEFT JOIN T_express e ON e.nid = m.expressnid " +
            "WHERE m.FilterFlag = 100 AND m.shippingmethod = 1 " +
            "AND m.addressowner='ebay' AND dt.storeId=7 AND ISNULL(m.trackNo,'')<>'' " +
            "AND e.name NOT IN ('万邑通','SpeedPAK') " +
            "AND CONVERT(VARCHAR(10),m.CLOSINGDATE,121) BETWEEN  %s AND %s " )
        self.cur.execute(sql,('2020-05-01','2020-05-03'))
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def update_ebay_tracking_number(self, data):
        for item in data:
            url = "https://api.ebay.com/sell/fulfillment/v1/order/" + item['ack'] + "/shipping_fulfillment"
            payload = {
                "lineItems" : [
                    {
                        "lineItemId": "string",
                        "quantity": "integer"}
                ],
                "shippedDate": "string",
                "shippingCarrierCode": "string",
                "trackingNumber": "string"
            }
            print(url)



    def run(self):
        try:
            data = self.get_order_data()
            self.update_ebay_tracking_number(data) #春节期间不转移
        except Exception as e:
            self.logger.error(e)
        finally:
            self.close()



if __name__ == '__main__':
    worker = Marker()
    worker.run()



