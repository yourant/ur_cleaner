#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-08-20 16:26
# Author: henry

import os
import datetime
from src.services.base_service import CommonService
from configs.config import Config
import phpserialize

from pymongo import MongoClient

mongo = MongoClient('192.168.0.150', 27017)
table = mongo['operation']['ebay_products']


class EbayFee(CommonService):
    """
    fetch ebay fee using api
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

        self.warehouse_name = 'mysql'
        self.warehouse_cur = self.base_dao.get_cur(self.warehouse_name)
        self.warehouse_con = self.base_dao.get_connection(self.warehouse_name)

    def close(self):
        self.base_dao.close_cur(self.cur)
        self.base_dao.close_cur(self.warehouse_cur)

    def get_ebay_ad_fee_from_py(self, item_id, begin, end):
        sql = ("SELECT notename AS suffix,currency_code AS ad_code,b.ExchangeRate AS ad_code_rate,fee_time,description,"
               "itemId, total AS ad_fee, "
               "CASE WHEN CHARINDEX('廣告費率：',memo) > 0  THEN SUBSTRING(memo,CHARINDEX('廣告費率：',memo) + 5,"
               "   CHARINDEX('%',memo) - CHARINDEX('廣告費率：',memo) - 5) "
               "WHEN CHARINDEX('Ad rate: ',memo) > 0 THEN SUBSTRING(memo,CHARINDEX('Ad rate: ',memo) + 8,"
               "   CHARINDEX('%',memo) - CHARINDEX('Ad rate: ',memo) - 8) "
               "ELSE '' END AS ad_rate, "
               "CASE WHEN CHARINDEX('成交價：GBP',memo) > 0 THEN 'GBP' "
               "WHEN CHARINDEX('成交價：US',memo) > 0 THEN 'USD' "
               "WHEN CHARINDEX('成交價：AU',memo) > 0 THEN 'AUD' "
               "WHEN CHARINDEX('成交價：EUR',memo) > 0 THEN 'EUR' "
               "WHEN CHARINDEX('成交價：C',memo) > 0 THEN 'CAD' "
               "WHEN CHARINDEX('sale price: gbp',LOWER(memo)) > 0 THEN 'GBP' "
               "WHEN CHARINDEX('sale price: us',LOWER(memo)) > 0 THEN 'USD' "
               "WHEN CHARINDEX('sale price: au',LOWER(memo)) > 0 THEN 'AUD' "
               "WHEN CHARINDEX('sale price: eur',LOWER(memo)) > 0 THEN 'EUR' "
               "WHEN CHARINDEX('sale price: c',LOWER(memo)) > 0 THEN 'CAD' "
               "WHEN CHARINDEX('&#163;',memo) > 0 THEN 'GBP' "
               "ELSE currency_code END AS transaction_code, c.ExchangeRate AS transaction_code_rate, "
               "CASE WHEN CHARINDEX('成交價：GBP',memo) > 0 THEN SUBSTRING(memo,CHARINDEX('成交價：GBP',memo) + 8, "
               "      CHARINDEX('廣告費率：',memo)  - CHARINDEX('成交價：GBP',memo) - 9) "
               "WHEN CHARINDEX('成交價：AU',memo) > 0 THEN SUBSTRING(memo,CHARINDEX('成交價：AU',memo) + 8, "
               "            CHARINDEX('廣告費率：',memo)  - CHARINDEX('成交價：AU',memo) - 9) "
               "WHEN CHARINDEX('成交價：US',memo) > 0 THEN SUBSTRING(memo,CHARINDEX('成交價：US',memo) + 8, "
               "            CHARINDEX('廣告費率：',memo)  - CHARINDEX('成交價：US',memo) - 9) "
               "WHEN CHARINDEX('成交價：EUR',memo) > 0 THEN SUBSTRING(memo,CHARINDEX('成交價：EUR',memo) + 8, "
               "            CHARINDEX('廣告費率：',memo)  - CHARINDEX('成交價：EUR',memo) - 9) "
               "WHEN CHARINDEX('成交價：C',memo) > 0 THEN SUBSTRING(memo,CHARINDEX('成交價：C',memo) + 7, "
               "            CHARINDEX('廣告費率：',memo)  - CHARINDEX('成交價：C',memo) - 8) "
               "WHEN CHARINDEX('sale price: au',LOWER(memo)) > 0 THEN SUBSTRING(memo,CHARINDEX('sale price: au',LOWER(memo)) + 16, "
               "            CHARINDEX('Ad rate:',memo)  - CHARINDEX('sale price: au',LOWER(memo)) - 18) "
               "WHEN CHARINDEX('sale price: us',LOWER(memo)) > 0 THEN SUBSTRING(memo,CHARINDEX('sale price: us',LOWER(memo)) + 16, "
               "            CHARINDEX('Ad rate:',memo)  - CHARINDEX('sale price: us',LOWER(memo)) - 18) "
               "WHEN CHARINDEX('sale price: gbp',LOWER(memo)) > 0 THEN SUBSTRING(memo,CHARINDEX('sale price: gbp',LOWER(memo)) + 16, "
               "            CHARINDEX('Ad rate:',memo)  - CHARINDEX('sale price: gbp',LOWER(memo)) - 18) "
               "WHEN CHARINDEX('sale price: c',LOWER(memo)) > 0 THEN SUBSTRING(memo,CHARINDEX('sale price: c',LOWER(memo)) + 15, "
               "            CHARINDEX('Ad rate:',memo)  - CHARINDEX('sale price: c',LOWER(memo)) - 17) "
               "WHEN CHARINDEX('sale price: &#163;',memo) > 0 THEN SUBSTRING(memo,CHARINDEX('sale price: &#163;',LOWER(memo)) + len('sale price: &#163;'), 																									"
               " CHARINDEX('Ad rate:',memo)  - CHARINDEX('sale price: &#163;',LOWER(memo)) - len('sale price: &#163;') - 2) "
               "ELSE '' END AS transaction_price, "
               "CASE WHEN CHARINDEX('按 ',memo) > 0 THEN SUBSTRING(memo,CHARINDEX('按 ',LOWER(memo)) + 2, 7)"
               " WHEN CHARINDEX('at a rate of ',memo) > 0 THEN SUBSTRING(memo,CHARINDEX('at a rate of ',LOWER(memo)) + 13, 7) "
               "ELSE 1.00000 END AS trans_code_to_ad_code_rate "
               "FROM y_fee (nolock) f "
               "LEFT JOIN B_CurrencyCode(nolock) b ON b.CURRENCYCODE=f.currency_code "
               "LEFT JOIN B_CurrencyCode(nolock) c ON c.CURRENCYCODE=(CASE WHEN CHARINDEX('成交價：GBP',memo) > 0 THEN 'GBP' "
               "WHEN CHARINDEX('成交價：US',memo) > 0 THEN 'USD' "
               "WHEN CHARINDEX('成交價：AU',memo) > 0 THEN 'AUD' "
               "WHEN CHARINDEX('成交價：EUR',memo) > 0 THEN 'EUR' "
               "WHEN CHARINDEX('成交價：C',memo) > 0 THEN 'CAD' "
               "WHEN CHARINDEX('sale price: gbp',LOWER(memo)) > 0 THEN 'GBP' "
               "WHEN CHARINDEX('sale price: us',LOWER(memo)) > 0 THEN 'USD' "
               "WHEN CHARINDEX('sale price: au',LOWER(memo)) > 0 THEN 'AUD' "
               "WHEN CHARINDEX('sale price: eur',LOWER(memo)) > 0 THEN 'EUR' "
               "WHEN CHARINDEX('sale price: c',LOWER(memo)) > 0 THEN 'CAD' "
               "WHEN CHARINDEX('&#163;',memo) > 0 THEN 'GBP' "
               "ELSE currency_code END) "
               "WHERE notename IN (SELECT DictionaryName FROM B_Dictionary nolock  "
               "          WHERE CategoryID=12 AND FitCode ='eBay')   "
               "AND description IN ('廣告費','Ad fee') "
               "AND itemId = %s AND fee_time BETWEEN %s AND %s"
               )
        self.cur.execute(sql, (item_id, begin, end))
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_ebay_item_from_py(self, begin, end):
        sql = ("SELECT DISTINCT itemId FROM y_fee (nolock) f "
               "WHERE notename IN (SELECT DictionaryName FROM B_Dictionary nolock "
               "WHERE CategoryID=12 AND FitCode ='eBay')"
               "AND fee_time BETWEEN %s AND %s "
               "AND description IN ('廣告費','Ad fee')"
               # "AND itemId = '363197260457' AND fee_time='2021-02-21 21:41:10'"
               )
        self.cur.execute(sql, (begin, end))
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_ebay_shipping_fee_from_mongo(self, item_id):
        out = {'sku': '', 'shipping_fee': 0, 'shipping_name': ''}
        try:
            ret = table.find_one({'itemID': item_id})
            if ret:
                out['sku'] = ret['parentSku'].split('@#')[0]
                item = ret['shippingDetails']['shippingServiceOptions']
                for k in item:
                    if k['shippingServicePriority'] == '1':
                        try:
                            out['shipping_fee'] = k['shippingServiceCost']['value']
                        except BaseException:
                            out['shipping_fee'] = 0
                        out['shipping_name'] = k['ShippingService']
                        break
            return out
        except Exception as e:
            self.logger.error(f"Failed to get shipping fee of item {item_id} cause of {e}")
            return out

    def get_ebay_shipping_fee_from_ibay(self, item_id):
        sql = ("SELECT sku,shippingdetails,ei.itemid FROM ebay_item ei "
               "LEFT JOIN ebay_item_detail eid ON ei.itemid=eid.itemid "
               "WHERE ei.itemid = %s")
        out = dict()
        try:
            self.ibay_cur.execute(sql, (str(item_id),))
            ret = self.ibay_cur.fetchone()
            print(ret)
            if ret:
                sku = ret[0].split('@#')[0]
                out['sku'] = sku
                shipping_str = bytes(ret[1], encoding='utf8')
                shipping = phpserialize.loads(shipping_str)
                # print(shipping)
                try:
                    item = shipping[b'ShippingServiceOptions']
                    for k in item:
                        if item[k][b'ShippingServicePriority'].decode('utf-8') == '1':

                            try:
                                out['shipping_fee'] = item[k][b'ShippingServiceAdditionalCost'].decode('utf-8')
                            except BaseException:
                                out['shipping_fee'] = item[k][b'ShippingServiceCost'].decode('utf-8')
                            out['shipping_name'] = item[k][b'ShippingService'].decode('utf-8')
                            break
                except BaseException:
                    out['shipping_fee'] = 0
                    out['shipping_name'] = ''
                return out
        except Exception as e:
            out['sku'] = ''
            out['shipping_fee'] = 0
            out['shipping_name'] = ''
            self.logger.error(f"Failed to get shipping fee of item {item_id} cause of {e}")
            return out

    def save_data(self, rows):
        try:
            sql = ("insert into cache_ebayAdFee(suffix, sku, ad_code, ad_code_rate, ad_fee, ad_rate, fee_time, "
                   "description, item_id, transaction_code, transaction_code_rate, trans_code_to_ad_code_rate, "
                   "transaction_price, shipping_fee, shipping_name, update_time) "
                   "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")
            self.warehouse_cur.executemany(sql, rows)
            self.warehouse_con.commit()
        except Exception as e:
            self.logger.info(f'failed to save ad fee info cause of {e}')

    def clean(self, begin_date, end_date):
        sql = 'delete from cache_ebayAdFee where fee_time between %s and %s'
        self.warehouse_cur.execute(sql, (begin_date, end_date))
        self.warehouse_con.commit()
        self.logger.info(f'success to clear sales data between {begin_date} and {end_date}')

    def run(self):
        try:
            today = str(datetime.datetime.today())
            begin = str(datetime.datetime.today() - datetime.timedelta(days=))[:10]
            end = str(today)[:10]
            # print(begin, end)
            self.clean(begin, end)
            data = self.get_ebay_item_from_py(begin, end)
            for item in data:
                res = []
                rows = self.get_ebay_ad_fee_from_py(item['itemId'], begin, end)
                # ship_info = self.get_ebay_shipping_fee_from_ibay(item['itemId'])
                ship_info = self.get_ebay_shipping_fee_from_mongo(item['itemId'])
                for row in rows:

                    ad_fee_list = (row['suffix'], ship_info['sku'], row['ad_code'], row['ad_code_rate'],
                                   row['ad_fee'], row['ad_rate'], row['fee_time'], row['description'],
                                   row['itemId'], row['transaction_code'], row['transaction_code_rate'],
                                   row['trans_code_to_ad_code_rate'], row['transaction_price'],
                                   ship_info['shipping_fee'], ship_info['shipping_name'], today)
                    res.append(ad_fee_list)
                    # print(res)
                self.save_data(res)

        except Exception as e:
            self.logger.error(e)
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == '__main__':
    worker = EbayFee()
    worker.run()
