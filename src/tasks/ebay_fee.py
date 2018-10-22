#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-19 16:26
# Author: turpure


import datetime
from concurrent.futures import ThreadPoolExecutor,as_completed
from tenacity import retry, stop_after_attempt
from ebaysdk.trading import Connection as Trading
from src.services import db, log


class EbayFee(object):
    """
    fetch ebay fee using api
    """
    def __init__(self):
        self.con = db.Mssql().connection
        self.logger = log.SysLogger().log

    def run_sql(self, sql):
        cur = self.con.cursor(as_dict=True)
        with cur as cr:
            cr.execute(sql)
            ret = cr.fetchall()
            for row in ret:
                yield row

    def get_ebay_token(self):
        sql = 'SELECT notename,max(ebaytoken) AS ebaytoken FROM S_PalSyncInfo  GROUP BY notename'
        return self.run_sql(sql)

    @retry(stop=stop_after_attempt(3))
    def get_ebay_fee(self, ebay_token):
        """
        get the ebay fee of yesterday in local time
        """
        begin_date = str(datetime.datetime.now() - datetime.timedelta(days=1))[:10]
        end_date = str(datetime.datetime.now())[:10]
        begin_date += "T00:00:00.000Z"
        end_date += "T00:00:00.000Z"  # utc time
        try:
            api = Trading(config_file='D:/ur_cleaner/configs/dev/ebay.yaml', timeout=40)
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
                                yield fee
                        break

                    except Exception as e:
                        self.logger.error("trying %s times but %s" % (i, e))
        except Exception as e:
            self.logger.error('%s while getting ebay fee' % e)
            raise Exception(e)

    def save_data(self, row):
        cur = self.con.cursor()
        sql = 'insert into ebayInsertionfee(accountname,insertionFee,createdday,feeType,itemid)' \
              ' values(%s,%s,%s,%s,%s)'
        try:
            with cur as cr:
                cr.execute(sql, (row['accountName'], row['value'],
                                 row['Date'], row['feeType'], row['ItemID']))
                self.logger.info("putting %s" % row['accountName'])
                self.con.commit()
        except Exception as e:
            self.logger.error("%s while trying to save data" % e)

    def save_trans(self, token):
        ret = self.get_ebay_fee(token)
        for row in ret:
            self.save_data(row)

    def run(self):
        tokens = self.get_ebay_token()
        # pool = ThreadPoolExecutor()
        # try:
        #     ret = pool.map(self.get_ebay_fee, tokens)
        #     for item in ret:
        #         for row in item:
        #             self.save_data(row)
        # except Exception as e:
        #     self.logger.error(e)
        with ThreadPoolExecutor() as pool:
            future = {pool.submit(self.get_ebay_fee, token): token for token in tokens}
            for fu in as_completed(future):
                try:
                    data = fu.result()
                    for row in data:
                        self.save_data(row)
                except Exception as e:
                    self.logger.error(e)


if __name__ == '__main__':
    worker = EbayFee()
    worker.run()




