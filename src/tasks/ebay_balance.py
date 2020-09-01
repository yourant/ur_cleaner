#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-19 16:26
# Author: turpure


import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from ebaysdk.trading import Connection as Trading
from ebaysdk.exception import ConnectionError
from src.services.base_service import CommonService
from configs.config import Config
import pymssql


class EbayBalance(CommonService):
    """
    fetch ebay account balance using api
    """

    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')
        self.base_name = 'mssql'
        self.warehouse = 'mysql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.warehouse_cur = self.base_dao.get_cur(self.warehouse)
        self.warehouse_con = self.base_dao.get_connection(self.warehouse)

    def close(self):
        self.base_dao.close_cur(self.cur)
        self.base_dao.close_cur(self.warehouse_cur)

    def get_ebay_token(self):
        sql = ("select sp.noteName,max(sp.ebaytoken) AS ebaytoken, bd.Used "
               "from S_PalSyncInfo(nolock)  as sp LEFT JOIN B_Dictionary(nolock) as bd "
               "on sp.NoteName = bd.DictionaryName "
               "where  bd.cateGoryId=12 and bd.fitCode='eBay' "
               "GROUP BY sp.noteName, bd.Used")
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

        now = datetime.datetime.utcnow()
        begin_date = now - datetime.timedelta(hours=2)
        now = now.strftime("%Y-%m-%dT%H:%M:%S") + '.000Z'
        begin_date = begin_date.strftime("%Y-%m-%dT%H:%M:%S") + '.000Z'
        par = {
            "RequesterCredentials": {"eBayAuthToken": ebay_token['ebaytoken']},
            "BeginDate": begin_date,
            "EndDate": now,
            'ExcludeBalance': 0,
            "Pagination": {"EntriesPerPage": 100, "PageNumber": 1}
        }

        return par

    def _get_ebay_balance(self, par):
        api = Trading(siteid=0, config_file=self.config, timeout=40)
        currency = ['GBP', 'USD']
        for cur in currency:
            response = None
            ret = None
            for _ in range(3):
                try:
                    par['Currency'] = cur
                    response = api.execute('GetAccount', par)
                    break
                except ConnectionError:
                    ret = {'currency': cur, 'balance': 0}
                    break
                except:
                    pass

            try:
                if response:
                    summary = response.reply.AccountSummary
                    ret = summary.CurrentBalance
                    yield ret
                else:
                    yield ret
            # to-do read time out exception
            except Exception as why:
                self.logger.error(why)

    def _parse_response(self, ret, ebay_token):
        # return ret
        out = {'accountName': ebay_token['noteName'], 'isUsed': ebay_token['Used']}
        if ret:
            if isinstance(ret, dict):
               out['currency'] = ret['currency']
               out['balance'] = ret['balance']
            else:
                out['currency'] = ret._currencyID
                out['balance'] = ret.value
                out['accountName'] = ebay_token['noteName']
            return out

    def save_data(self, row):
        sql = ('insert into ebay_balance(accountName,balance,currency,updatedDate, isUsed)'
              ' values(%s,%s,%s,now(),%s) on duplicate key '
               'update balance=values(balance), updatedDate=now(), isUsed=values(isUsed)')
        try:
            self.warehouse_cur.execute(sql, (row['accountName'], row['balance'], row['currency'], row['isUsed']))
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




