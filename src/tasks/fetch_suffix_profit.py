#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-11-08 13:04
# Author: turpure

import datetime
from src.services.base_service import BaseService


class ProfitFetcher(BaseService):
    """
    fetch suffix profit from erp day by day
    """

    def __init__(self):
        super().__init__()

    def fetch(self, date_flag, begin_date, end_date):
        sql = 'oauth_FinancialProfit @dateFlag=%s, @beginDate=%s, @endDate=%s'
        self.cur.execute(sql, (date_flag, begin_date, end_date))
        ret = self.cur.fetchall()
        for row in ret:
            yield (
                row['suffix'], row['plat'], row['SaleMoney'], row['salemoneyzn'], row['eBayFeeebay'], row['eBayFeeznebay'],
                row['ppFee'], row['ppFeezn'], row['CostMoney'], row['ExpressFare'], row['InpackageMoney'],
                row['StoreName'], row['refund'], row['diefeeZn'], row['suffixFee'], row['opeFee'],
                row['grossProfit'], row['orderDay'], row['dateFlag']
            )

    def push(self, rows):
        sql = ['insert into cache_suffixProfit(',
               'suffix,plat,saleMoney,saleMoneyZn,ebayFeeEbay,',
               'ebayFeeZnEbay,ppFee,ppFeeZn,costMoney,',
               'expressFare,inPackageMoney,storeName,',
               'refund,dieFeeZn,insertionFee,saleOpeFeeZn,',
               'grossProfit,createdDate,dateFlag',
               ') values (',
               '%s,%s,%s,%s,%s,',
               '%s,%s,%s,%s,%s,',
               '%s,%s,%s,%s,%s,',
               '%s,%s,%s,%s',
               ') ON DUPLICATE KEY UPDATE saleMoney=values(saleMoney),plat=values(plat),'
               'saleMoneyZn=values(saleMoneyZn),ebayFeeEbay=values(ebayFeeEbay),'
               'ebayFeeZnEbay=values(ebayFeeZnEbay),ppFee=values(ppFee),'
               'ppFeeZn=values(ppFeeZn),costMoney=values(costMoney),'
               'expressFare=values(expressFare),inPackageMoney=values(inPackageMoney),'
               'refund=values(refund),dieFeeZn=values(dieFeeZn),insertionFee=values(insertionFee),'
               'saleOpeFeeZn=values(saleOpeFeeZn),grossProfit=values(grossProfit)'
               ]
        self.warehouse_cur.executemany(''.join(sql), rows)
        self.warehouse_con.commit()
        self.logger.info('success to fetch suffix profit')

    def clear(self, begin, end):
        sql = 'delete from cache_suffixProfit where createdDate BETWEEN %s and  %s'
        self.warehouse_cur.execute(sql, (begin, end))
        self.warehouse_con.commit()
        self.logger.info('success to clear suffix profit')

    def work(self):
        try:
            yesterday = str(datetime.datetime.today() - datetime.timedelta(days=1))[:10]
            # yesterday = '2019-12-01'
            today = str(datetime.datetime.today())[:10]
            month_first_day = str(datetime.datetime.strptime(yesterday[:8] + '01', '%Y-%m-%d'))[:10]
            self.clear(month_first_day, today)
            for date_flag in (0, 1):
                rows = self.fetch(date_flag, month_first_day, today)
                self.push(rows)
        except Exception as why:
            self.logger.error('fail to fetch suffix profit cause of {}'.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    worker = ProfitFetcher()
    worker.work()
