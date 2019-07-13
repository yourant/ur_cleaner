#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-19 16:26
# Author: turpure


import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tenacity import retry, stop_after_attempt
from ebaysdk.trading import Connection as Trading
from ebaysdk import exception
from src.services.base_service import BaseService
from configs.config import Config
import pymssql


class EbayBalance(BaseService):
    """
    fetch ebay account balance using api
    """
    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')

    def get_ebay_token(self):
        sql = ("SELECT noteName,max(ebaytoken) AS ebaytoken "
               "FROM S_PalSyncInfo  GROUP BY notename")
               # " having notename='eBay-A6-vitalityang1'")
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_ebay_balance(self, ebay_token):
        par = self.get_request_params(ebay_token)
        for ret in self._get_ebay_balance(par):
             yield self._parse_response(ret, ebay_token)

    @staticmethod
    def get_request_params(ebay_token):
        """
        get the ebay balance of yesterday in local time
        """

        end_date = str(datetime.datetime.now())[:10]
        # begin_date = str(datetime.datetime.now() - datetime.timedelta(days=-1))[:10]
        begin_date = end_date
        begin_date += "T00:00:00.000Z"
        end_date += "T01:00:00.000Z"  # utc time
        par = {
            "RequesterCredentials": {"eBayAuthToken": ebay_token['ebaytoken']},
            "BeginDate": begin_date,
            "EndDate": end_date,
            "Pagination": {"EntriesPerPage": 2000, "PageNumber": 1}
        }

        return par

    def _get_ebay_balance(self, par):
        api = Trading(siteid=0, config_file=self.config, timeout=40)
        currency = ['GBP', 'USD', 'HKD']
        for cur in currency:
            try:
                par['Currency'] = cur
                response = api.execute('GetAccount', par)
                try:
                    ret = response.reply.AccountSummary.AdditionalAccount[0]
                    yield ret

                except Exception as why:
                    self.logger.error('error while getting accountEntry cause of {}'.format(why))
                # to-do read time out exception
            except Exception as why:
                self.logger.error(why)

    def _parse_response(self, ret, ebay_token):
        # return ret
        out = dict()
        out['currency'] = ret.Currency
        out['balance'] = ret.Balance.value
        out['accountName'] = ebay_token['noteName']
        return out

    def save_data(self, row):
        sql = ('insert into ebay_balance(accountName,balance,currency,updatedDate)'
              ' values(%s,%s,%s,now()) on duplicate key update balance=values(balance), updatedDate=now()')
        try:
            self.warehouse_cur.execute(sql, (row['accountName'], row['balance'], row['currency'],))
            self.logger.info("putting %s" % row['accountName'])
            self.warehouse_con.commit()
        except pymssql.IntegrityError as e:
            pass
        except Exception as e:
            self.logger.error("%s while trying to save data" % e)

    def save_trans(self, token):
        ret = self.get_ebay_fee(token)
        for row in ret:
            self.save_data(row)

    def run(self):
        try:
            tokens = self.get_ebay_token()
            with ThreadPoolExecutor(16) as pool:
                future = {pool.submit(self.get_ebay_balance, token): token for token in tokens}
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
    worker = EbayBalance()
    worker.run()




