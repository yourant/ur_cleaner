#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-20 10:02
# Author: turpure

import datetime
from src.services.base_service import CommonService


class FileOrdersToHis(CommonService):
    """
    get refunded orders of wish
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

    def get_batch_number(self):
        sql = "exec  P_S_CodeRuleGet 230,'';"
        self.cur.execute(sql)
        ret = self.cur.fetchone()
        return ret['MaxBillCode']

    def get_order_ids(self, begin, end):
        sql = "SELECT nid FROM P_Trade (nolock) WHERE FilterFlag = 100 AND CONVERT(VARCHAR(10),CLOSINGDATE,121) BETWEEN  %s AND %s"
        self.cur.execute(sql,(begin, end))
        ret = self.cur.fetchall()
        for row in ret:
            yield row


    def run(self):
        now = datetime.datetime.now()
        begin = '2019-01-01'
        end = datetime.datetime(now.year, now.month - 2, 1) - datetime.timedelta(1)
        # end = '2020-03-31'
        try:
            ids = self.get_order_ids(begin, end)
            batch_number = self.get_batch_number()
            item = []
            id_dict = []
            step = 50
            i = 0
            # len = sum(1 for _ in ids)
            for id in ids:
                item.append(str(id['nid']))
                if i != 0 and i%step == 0 :
                    id_dict.append(str(','.join(item)))
                    item = []
                i = i + 1
            for id in id_dict:
                sql = "exec P_ForwardTradeToHis %s,%s,'ur-cleaner'"
                self.cur.execute(sql, (id, batch_number))
                self.con.commit()


        except Exception as e:
            self.logger.error(e)
        finally:
            self.close()


if __name__ == "__main__":
    import time
    start = time.time()
    worker = FileOrdersToHis()
    worker.run()
    end = time.time()
    date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
    print(date + f' it takes {end - start} seconds')




