#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-03-28 10:32
# Author: turpure


from src.services.base_service import BaseService


class EbayVirtual(BaseService):
    """
    get ebay order tracking info
    """
    def __init__(self):
        super().__init__()

    def run(self):
        try:
            sql = 'EXEC B_eBayOversea_ModifyOnlineNumberOnTheIbay365'
            self.cur.execute(sql)
            self.cur.commit()
        except Exception as why:
            self.logger.error(why)
        finally:
            self.close()


if __name__ == '__main__':
    work = EbayVirtual()
    work.run()














