#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-07-22 11:30
# Author: turpure

import os
from src.services.base_service import CommonService
from pymssql import IntegrityError


class Worker(CommonService):
    """
    sync payPal account from  puYuan to ur center
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.warehouse = 'mysql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.warehouse_cur = self.base_dao.get_cur(self.warehouse)
        self.warehouse_con = self.base_dao.get_connection(self.warehouse)

    def close(self):
        self.base_dao.close_cur(self.cur)
        self.base_dao.close_cur(self.warehouse_cur)

    def get_paypal_accounts(self):
        sql = 'SELECT top 20 e_maill as email, max(syncinfoId) as nid FROM [dbo].[S_PalSyncInfo](nolock) GROUP BY e_maill ORDER BY nid desc '
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def put_paypal_accounts(self, ele):
        sql = ("insert into y_PayPalStatus (accountName,isUrUsed, isPyUsed,paypalStatus,memo,createdTime,updatedTime) "
               "values(%s,%s,%s,%s,%s,getdate(),getdate())")
        try:
            self.cur.execute(sql, (ele['email'], 0, 1, '使用中', ''))
            self.logger.info(f'success to put {ele["email"]}')

        except IntegrityError:
            sql = (
                "update y_PayPalStatus set isPyUsed = 1,updatedTime= getdate()  where accountName=%s"
            )
            self.cur.execute(sql, (ele["email"]))
            self.logger.info(f'success to update  {ele["email"]}')

        except Exception as why:
            self.logger.error(f'fail to put {ele["email"]} cause of {why}')

    def trans(self):
        accounts = self.get_paypal_accounts()
        for ele in accounts:
            self.put_paypal_accounts(ele)
        self.con.commit()

    def work(self):
        try:
            self.trans()
        except Exception as why:
            self.logger.error('fail to finish task cause of {} '.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.work()


