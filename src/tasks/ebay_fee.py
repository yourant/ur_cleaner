#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-19 16:26
# Author: turpure


import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt
from ebaysdk.trading import Connection as Trading
from src.services.base_service import BaseService
from configs.config import Config


class EbayFee(BaseService):
    """
    fetch ebay fee using api
    """
    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')
        if not self._get_batch_id():
            self.batch_id = str(datetime.datetime.now() - datetime.timedelta(days=1))[:10]
        else:
            self.batch_id = str(datetime.datetime.strptime(self._get_batch_id(), '%Y-%m-%d')
                                + datetime.timedelta(days=1))[:10]

    def _get_batch_id(self):
        sql = 'select max(batchId) as batchId from y_fee'
        try:
            self.cur.execute(sql)
            ret = self.cur.fetchone()
            return ret['batchId']
        except Exception as why:
            self.logger.error('fail to get max batchId cause of {}'.format(why))

    def get_ebay_token(self):
        sql = 'SELECT notename,max(ebaytoken) AS ebaytoken FROM S_PalSyncInfo  GROUP BY notename'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    @retry(stop=stop_after_attempt(3))
    def get_ebay_fee(self, ebay_token):
        """
        get the ebay fee of yesterday in local time
        """

        # begin_date = str(datetime.datetime.now() - datetime.timedelta(days=1))[:10]
        begin_date = self.batch_id
        end_date = str(datetime.datetime.now())[:10]
        begin_date += "T00:00:00.000Z"
        end_date += "T00:00:00.000Z"  # utc time
        try:
            api = Trading(config_file=self.config, timeout=40)
            par = {
                "RequesterCredentials": {"eBayAuthToken": ebay_token['ebaytoken']},
                "AccountEntrySortType": "AccountEntryFeeTypeAscending",
                "AccountHistorySelection": "BetweenSpecifiedDates",
                "BeginDate": begin_date,
                "EndDate": end_date,
                "Pagination": {"EntriesPerPage": 2000, "PageNumber": 1}
            }
            response = api.execute('GetAccount', par)
            total_pages = int(response.reply.PaginationResult.TotalNumberOfPages)
            for num in range(0, total_pages):
                par['Pagination']['PageNumber'] = num + 1
                res = api.execute('GetAccount', par)
                for i in range(1, 4):
                    try:
                        ret = res.reply.AccountEntries.AccountEntry
                        for row in ret:
                            fee_type = row.AccountDetailsEntryType

                            if fee_type not in ('FeeFinalValue', 'FeeFinalValueShipping',
                                                'PayPalOTPSucc',
                                                'PaymentCC', 'CreditFinalValue', 'CreditFinalValueShipping', 'Unknown'
                                                ):
                                fee = dict()
                                fee['feeType'] = fee_type
                                fee['Date'] = str(row.Date)
                                fee['value'] = row.NetDetailAmount.value
                                fee['accountName'] = ebay_token['notename']
                                fee['ItemID'] = row.ItemID
                                if float(row.NetDetailAmount.value) >= 10 or float(row.NetDetailAmount.value) <= -10:
                                    self.logger.warning('%s:%s' % (fee_type, float(row.NetDetailAmount.value)))
                                if int(row.NetDetailAmount.value) != 0:
                                    yield fee
                        break

                    except Exception as e:
                        self.logger.error("trying %s times but %s" % (i, e))
        except Exception as e:
            self.logger.error('%s while getting ebay fee' % e)
            raise Exception(e)

    def save_data(self, row):
        sql = 'insert into y_fee(notename,fee_type,total,currency_code,fee_time,batchId)' \
              ' values(%s,%s,%s,%s,%s,%s)'
        try:
            self.cur.execute(sql, (row['accountName'], row['feeType'], row['value'], 'USD',
                             row['Date'], self.batch_id))
            self.logger.info("putting %s" % row['accountName'])
            self.con.commit()
        except Exception as e:
            self.logger.error("%s while trying to save data" % e)

    def save_trans(self, token):
        ret = self.get_ebay_fee(token)
        for row in ret:
            self.save_data(row)

    def run(self):
        try:
            tokens = self.get_ebay_token()
            with ThreadPoolExecutor() as pool:
                future = {pool.submit(self.get_ebay_fee, token): token for token in tokens}
                for fu in as_completed(future):
                    try:
                        data = fu.result()
                        for row in data:
                            self.save_data(row)
                    except Exception as e:
                        self.logger.error(e)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.close()


if __name__ == '__main__':
    worker = EbayFee()
    worker.run()




