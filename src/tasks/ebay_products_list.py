from ebaysdk.trading import Connection as Trading
from ebaysdk import exception
import datetime
import time
from src.services.base_service import CommonService
from configs.config import Config
from multiprocessing.pool import ThreadPool as Pool
from pymongo import MongoClient
import math
import os

mongo = MongoClient('192.168.0.150', 27017)
mongodb = mongo['ebay']
col = mongodb['ebay_product_list']


class FetchEbayLists(CommonService):
    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    def getData(self, token):
        suffix = token['suffix']
        i = 0
        try:
            trade_api = Trading(config_file=self.config)
            while True:
                fromDay = str(datetime.datetime.today() +
                              datetime.timedelta(days=120 * i))[:10]
                endDay = str(datetime.datetime.today() +
                             datetime.timedelta(days=120 * (i + 1) - 1))[:10]
                # print(fromDay,endDay)
                trade_response = trade_api.execute(
                    'GetSellerList',
                    {
                        'EndTimeFrom': fromDay,
                        'EndTimeTo': endDay,
                        'requesterCredentials': {'eBayAuthToken': token['token']},
                    }
                )
                trade_res = trade_response.dict()
                try:
                    dataNum = int(
                        trade_res['PaginationResult']['TotalNumberOfEntries'])
                except BaseException:
                    dataNum = 0

                if trade_res['Ack'] == 'Success' and dataNum > 0:
                    item = trade_res['ItemArray']['Item']
                    if isinstance(item, list):
                        for row in item:
                            self._parse_vars(trade_api, row, suffix, token['token'])
                            # print(row)
                    else:
                        self._parse_vars(trade_api, item, suffix, token['token'])
                else:
                    break
                i += 1
        except exception.ConnectionError as e:
            self.logger.error(
                'Suffix {} connect to failed cause of {}'.format(
                    suffix, e))

    def _parse_vars(self, api, row, suffix, token):
        try:
            # response = api.execute('GetItem', {'ItemID': row['ItemID']})
            response = api.execute('GetItem', {'ItemID': row['ItemID'], 'requesterCredentials': {'eBayAuthToken': token}})
            result = response.dict()
            if result['Ack'] == 'Success':
                try:
                    code = result['Item']['SKU'].replace("'", "")
                except BaseException:
                    code = ''
                # 多属性
                if 'Variations' in result['Item']:
                    Variation = result['Item']['Variations']['Variation']
                    if isinstance(Variation, list):
                        for item in result['Item']['Variations']['Variation']:
                            # print(item)
                            try:
                                newSku = item['SKU'].split(
                                    '@#')[0].split('*')[0].replace("'", "")
                            except BaseException:
                                newSku = item['SKU']
                            try:
                                paypal = result['Item']['PayPalEmailAddress']
                            except BaseException:
                                paypal = ''
                            try:
                                sku = item['SKU'].replace("'", "")
                            except BaseException:
                                sku = ''
                            ele = {
                                'code': code,
                                'sku': sku,
                                'newSku': newSku,
                                'itemid': result['Item']['ItemID'],
                                'suffix': suffix,
                                'selleruserid': result['Item']['Seller']['UserID'],
                                'storage': item['Quantity'] - item['SellingStatus']['QuantitySold'],
                                'listingType': result['Item']['ListingType'],
                                'country': result['Item']['Country'],
                                'paypal': paypal,
                                'site': result['Item']['Site'],
                                'updateTime': str(datetime.datetime.today())[:10]
                            }
                            col.insert_one(ele)
                            # self.save_data(ele)
                    else:
                        try:
                            newSku = Variation['SKU'].split(
                                '@#')[0].split('*')[0].replace("'", "")
                        except BaseException:
                            newSku = Variation['SKU']
                        try:
                            paypal = result['Item']['PayPalEmailAddress']
                        except BaseException:
                            paypal = ''
                        try:
                            sku = Variation['SKU'].replace("'", "")
                        except BaseException:
                            sku = ''

                        ele = {
                            'code': code,
                            'sku': sku,
                            'newSku': newSku,
                            'itemid': result['Item']['ItemID'],
                            'suffix': suffix,
                            'selleruserid': result['Item']['Seller']['UserID'],
                            # 'storage': Variation['Quantity'],
                            'storage': Variation['Quantity'] - Variation['SellingStatus']['QuantitySold'],
                            'listingType': result['Item']['ListingType'],
                            'country': result['Item']['Country'],
                            'paypal': paypal,
                            'site': result['Item']['Site'],
                            'updateTime': str(datetime.datetime.today())[:10]
                        }

                        col.insert_one(ele)
                        # self.save_data(ele)
                else:  # 单属性
                    try:
                        newSku = result['Item']['SKU'].split(
                            '@#')[0].split('*')[0].replace("'", "")
                    except BaseException:
                        newSku = result['Item']['SKU']
                    # print(item)
                    try:
                        paypal = result['Item']['PayPalEmailAddress']
                    except BaseException:
                        paypal = ''
                    try:
                        sku = result['Item']['SKU'].replace("'", "")
                    except BaseException:
                        sku = ''
                    ele = {
                        'code': code,
                        'sku': sku,
                        'newSku': newSku,
                        'itemid': result['Item']['ItemID'],
                        'suffix': suffix,
                        'selleruserid': result['Item']['Seller']['UserID'],
                        # 'storage': result['Item']['Quantity'],
                        'storage': result['Item']['Quantity'] - result['Item']['SellingStatus']['QuantitySold'],
                        'listingType': result['Item']['ListingType'],
                        'country': result['Item']['Country'],
                        'paypal': paypal,
                        'site': result['Item']['Site'],
                        'updateTime': str(datetime.datetime.today())[:10]
                    }
                    col.insert_one(ele)
                    # self.save_data(ele)
        except Exception as e:
            self.logger.error(
                'Suffix {} get listing detail failed cause of {}'.format(
                    suffix, e))

    def save_data(self, row):
        sql = f'insert into ibay365_ebay_lists(code,sku,newsku,itemid,suffix,selleruserid,storage,listingType,country,paypal,site,updateTime) ' \
            'values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        try:
            self.cur.execute(sql, (row['code'], row['sku'], row['newSku'], row['itemid'], row['suffix'], row['selleruserid'], row['storage'],
                                   row['listingType'], row['country'], row['paypal'], row['site'], row['updateTime']))
            self.con.commit()
        except Exception as why:
            self.logger.error(f"fail to save sku {row} cause of {why} ")

    def get_ebay_token(self):
        sql = ('SELECT  NoteName AS suffix,EuSellerID AS storeName, MIN(EbayTOKEN) AS token '
               'FROM [dbo].[S_PalSyncInfo] WHERE SyncEbayEnable=1 '
               'and notename in (select dictionaryName from B_Dictionary '
               "where  CategoryID=12 and FitCode ='eBay' and used = 0) "
               "and NoteName in ('eBay-39-abovestair5', '') "
               # "and NoteName in ('eBay-C86-syho_63','eBay-C91-heir918','eBay-C92-ha199597','eBay-C79-jlh-79',
               # 'eBay-C99-tianru98','eBay-C95-shi_7040','eBay-C96-sysy_3270','eBay-E23-sarodyconsulting134',
               # 'eBay-E37-howa589680','eBay-E38-cameron878_2','eBay-E39-berr_9671','eBay-33-moonstair8','eBay-34-starstair9')"
               # "and NoteName='eBay-38-followsun5' "
               "GROUP BY NoteName,EuSellerID ORDER BY NoteName ;")
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def clean(self):
        col.delete_many({})
        sql = "truncate table ibay365_ebay_lists"
        self.cur.execute(sql)
        self.con.commit()
        self.logger.info('success to clear ebay product list')

    def save_trans(self):
        rows = self.pull()
        self.push_batch(rows)
        mongo.close()

    @staticmethod
    def pull():
        # rows = col.find({'sku':{'$regex':"8C1085"}})
        rows = col.find()
        for row in rows:
            yield (row['code'], row['sku'], row['newSku'], row['itemid'], row['suffix'], row['selleruserid'],
                   row['storage'], row['listingType'], row['country'], row['paypal'], row['site'], row['updateTime'])

    def push_batch(self, rows):
        try:
            rows = list(rows)
            number = len(rows)
            step = 1000
            end = math.ceil(number / step)
            for i in range(0, end):
                value = ','.join(
                    map(str, rows[i * step: min((i + 1) * step, number)]))
                sql = f'insert into ibay365_ebay_lists(code,sku,newsku,itemid,suffix,selleruserid,storage,listingType,country,paypal,site,updateTime) values {value}'
                try:
                    self.cur.execute(sql)
                    self.con.commit()
                    self.logger.info(
                        f"success to save data of ebay products from {i * step} to  {min((i + 1) * step, number)}")
                except Exception as why:
                    self.logger.error(
                        f"fail to save data of ebay products cause of {why} ")
        except Exception as why:
            self.logger.error(f"fail to save ebay products cause of {why} ")

    def run(self):
        BeginTime = time.time()
        try:
            tokens = self.get_ebay_token()
            self.clean()
            pl = Pool(50)
            pl.map(self.getData, tokens)
            pl.close()
            pl.join()
            self.save_trans()
        except Exception as e:
            self.logger.error(e)
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()
        print('程序耗时{:.2f}'.format(time.time() - BeginTime))  # 计算程序总耗时


# 执行程序
if __name__ == "__main__":
    worker = FetchEbayLists()
    worker.run()
