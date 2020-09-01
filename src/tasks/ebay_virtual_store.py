#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-03-28 10:32
# Author: turpure

import os
from src.services.base_service import CommonService


class EbayVirtual(CommonService):
    """
    get ebay order tracking info
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    def run(self):
        try:
            sql = 'EXEC B_eBayOversea_ModifyOnlineNumberOnTheIbay365'
            self.cur.execute(sql)
            self.cur.commit()
        except Exception as why:
            self.logger.error(why)
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == '__main__':
    work = EbayVirtual()
    work.run()














