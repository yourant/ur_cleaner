#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-11-20 15:41
# Author: turpure


from src.services.base_service import CommonService


class Worker(CommonService):

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.col = self.get_mongo_collection('product_engine', 'ebay_stores')

    def get_stores(self):
        sql = 'SELECT distinct eBayUserID,NoteName FROM S_PalSyncInfo'
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        return ret

    def save(self, rows):
        for row in rows:
            try:
                self.col.insert(row)
            except Exception as why:
                self.logger.debug(f'fail to save {row["NoteName"]} cause of {why}')

    def run(self):
        try:
            stores = self.get_stores()
            self.save(stores)
            self.logger.info(f'success to sync ebay stores')

        except Exception as why:
            self.logger.error(f'fail to sync ebay stores cause of {why}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = Worker()
    worker.run()
