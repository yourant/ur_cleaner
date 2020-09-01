#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-19 16:26
# Author: turpure


import datetime
from ebaysdk.trading import Connection as Trading
from ebaysdk import exception
from src.services.base_service import BaseService
from configs.config import Config
from pymongo import MongoClient
from multiprocessing.pool import ThreadPool as Pool

mongo = MongoClient('192.168.0.172', 27017)
mongodb = mongo['operation']
col = mongodb['ebay_fee']


class EbayFee(BaseService):
    """
    fetch ebay fee using api
    """

    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')
        self.batch_id = str(datetime.datetime.now() - datetime.timedelta(days=7))[:10]
        # self.batch_id = '2020-08-01'

    def get_ebay_token(self):
        sql = ("SELECT notename,max(ebaytoken) AS ebaytoken FROM S_PalSyncInfo"
               " where notename in (select dictionaryName from B_Dictionary "
               "where  CategoryID=12 and FitCode ='eBay' and used = 0 ) and "
               " notename not in ('01-buy','11-newfashion','eBay-12-middleshine', '10-girlspring',"
               "'eBay-C105-jkl-27','eBay-E48-tys2526','eBay-E50-Haoyiguoji')"
               "  GROUP BY notename"
               # " having notename='06-happygirl'"
               )
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_request_params(self, ebay_token):
        """
        get the ebay fee of yesterday in local time
        """

        begin_date = self.batch_id
        end_date = str(datetime.datetime.now())[:10]
        if begin_date > end_date:
            begin_date = str(datetime.datetime.now() - datetime.timedelta(days=2))[:10]
        # begin_date = '2020-08-28'
        # end_date = '2020-08-29'
        begin_date += "T00:00:00.000Z"
        end_date += "T01:00:00.000Z"  # utc time
        par = {
            "RequesterCredentials": {"eBayAuthToken": ebay_token['ebaytoken']},
            "AccountEntrySortType": "AccountEntryCreatedTimeDescending",
            "AccountHistorySelection": "BetweenSpecifiedDates",
            "BeginDate": begin_date,
            "EndDate": end_date,
            "Pagination": {"EntriesPerPage": 2000, "PageNumber": 1}
        }
        return par

    def _get_ebay_fee(self, par, ebay_token):
        api = Trading(siteid=0, config_file=self.config, timeout=40)
        currency = ['USD', 'GBP']
        for cur in currency:
            try:
                par['Currency'] = cur
                response = None

                # 网络出错就重试俩次
                for i in range(2):
                    try:
                        response = api.execute('GetAccount', par)
                        break
                    except exception.ConnectionError as why:
                        self.logger.warning(f'error while request GetAccount of {ebay_token["notename"]} '
                                            f'with bill of {cur} cause of {why}')
                        break
                    except Exception as why:
                        self.logger.error(f'trying {i + 1} times to get accountEntry cause of {why}')
                if response:
                    total_pages = int(response.reply.PaginationResult.TotalNumberOfPages)
                    if not hasattr(response.reply, 'AccountEntries'):
                        self.logger.warning(f'{ebay_token["notename"]} has no bill of {cur}')
                    else:
                        if total_pages == 1:
                            ret = response.reply.AccountEntries.AccountEntry
                            ret = self._parse_response(ret, ebay_token)
                            for row in ret:
                                self.save(row)
                        if total_pages > 1:
                            ret = response.reply.AccountEntries.AccountEntry
                            ret = self._parse_response(ret, ebay_token)
                            for row in ret:
                                self.save(row)
                            for page in range(1, total_pages):
                                par['Pagination']['PageNumber'] = page
                                current_response = None

                                # 如果网络出错就重试俩次
                                for i in range(2):
                                    try:
                                        current_response = api.execute('GetAccount', par)
                                        break
                                    except exception.ConnectionError as why:
                                        self.logger.warning(f'error while getting accountEntry cause of {why}')
                                        break
                                    except Exception as why:
                                        self.logger.error(f'trying {i + 1} times to get accountEntry cause of {why}')
                                if current_response:
                                    ret = current_response.reply.AccountEntries.AccountEntry
                                    ret = self._parse_response(ret, ebay_token)
                                    for row in ret:
                                        self.save(row)
            except exception.ConnectionError as why:
                self.logger.error('error while getting accountEntry cause of {}'.format(why))

    def _parse_response(self, ret, ebay_token):
        for row in ret:
            fee_type = row.AccountDetailsEntryType
            record_id = row.get('RefNumber', '')
            if fee_type not in ('FeeFinalValue', 'FeeFinalValueShipping', 'PayPalOTPSucc', 'PaymentCCOnce', 'PaymentCC',
                                'CreditFinalValue', 'CreditFinalValueShipping', 'Unknown') and record_id != '0':
                fee = dict()
                fee['feeType'] = fee_type
                fee['description'] = row.get('Description', '')
                fee['itemId'] = row.get('ItemID', '')
                fee['memo'] = row.get('Memo', '')
                fee['transactionId'] = row.get('TransactionID', '')
                fee['orderId'] = row.get('OrderId', '')
                fee['recordId'] = record_id
                fee['Date'] = str(row.Date)
                fee['value'] = row.NetDetailAmount.value
                fee['currency'] = row.NetDetailAmount._currencyID
                fee['accountName'] = ebay_token['notename']
                if float(row.NetDetailAmount.value) >= 10 or float(row.NetDetailAmount.value) <= -10:
                    self.logger.warning('%s:%s' % (fee_type, float(row.NetDetailAmount.value)))
                if float(row.NetDetailAmount.value) != 0:
                    yield fee

    @staticmethod
    def save(row):
        col.update_one({'recordId': row['recordId']}, {"$set": row}, upsert=True)

    def work(self, ebay_token):
        par = self.get_request_params(ebay_token)
        try:
            self._get_ebay_fee(par, ebay_token)
            self.logger.info(f'success to finish job of getting fee of {ebay_token["notename"]}')
        except Exception as why:
            self.logger.error(f'fail to work in get fee of {ebay_token["notename"]} cause of {why}')

    @staticmethod
    def get_data(begin, end):
        rows = col.find({'Date': {'$gte': begin, '$lte': end}})
        for row in rows:
            yield (row['accountName'], row['feeType'], row['value'], row['currency'],
                   row['Date'], str(row['Date'])[:10], row['description'], row['itemId'], row['memo'],
                   row['transactionId'], row['orderId'], row['recordId'])

    def insert(self, row):
        sql = (
            'insert into y_fee(notename,fee_type,total,currency_code,fee_time,batchId,description,itemId,memo,'
            'transactionId,orderId,recordId) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '
        )
        try:
            self.cur.executemany(sql, row)
            self.con.commit()
        except Exception as e:
            self.logger.error("%s while trying to save data" % e)

    def clean(self, begin, end):
        sql = ("DELETE FROM [dbo].[y_fee] WHERE notename IN "
               "(SELECT dictionaryName FROM B_Dictionary WHERE  CategoryID=12 AND FitCode ='eBay' AND used = 0) "
               " AND fee_time >= %s  AND fee_time < %s ")
        self.cur.execute(sql, (begin, end))
        self.con.commit()
        self.logger.info(f'success to clean fee data from y_fee  time between {begin} and {end}')

    def insert_to_sql(self):
        begin = self.batch_id
        end = str(datetime.datetime.now())[:10]
        self.clean(begin, end)
        rows = self.get_data(begin, end)
        # for row in rows:
        #     print(row)
        self.insert(rows)

    def run(self):
        try:
            tokens = self.get_ebay_token()
            with Pool(4) as pl:
                pl.map(self.work, tokens)

            self.insert_to_sql()
        except Exception as e:
            self.logger.error(e)
        finally:
            self.close()
            mongo.close()


if __name__ == '__main__':
    worker = EbayFee()
    worker.run()
