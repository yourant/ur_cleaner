#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-11-28 9:45
# Author: turpure

from src.services.base_service import BaseService


class StockReporter(BaseService):
    """
    report out of stock info
    """
    def __init__(self):
        super().__init__()

    def report(self):
        sql = ("EXEC oauth_outOfStockSku @GoodsState='',@MoreStoreID='',"
               "@GoodsUsed='0',@SupplierName='',@WarningCats='',@MoreSKU='',"
               "@cg=0,@GoodsCatsCode='',@index='1',@KeyStr='',@PageNum='100',"
               "@PageIndex='1',@Purchaser='',@LocationName='',@Used=''")
        self.cur.execute(sql)
        self.con.commit()

    def work(self):
        try:
            self.report()
            self.logger.info('update out of stock info')
        except Exception as e:
            self.logger.error(e)
        finally:
            self.close()


if __name__ == "__main__":
    reporter = StockReporter()
    reporter.work()
