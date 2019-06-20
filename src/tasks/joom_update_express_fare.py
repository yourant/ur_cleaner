#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-06-20 20:02
# Author: turpure


from src.services.base_service import BaseService
import datetime


class Updater(BaseService):

    def get_data(self, begin_date, end_date):
        sql = 'oa_p_joomNullExpressFare %s, %s'
        self.cur.execute(sql, (begin_date, end_date))
        self.con.commit()

    def update_date(self):
        sql = 'oa_p_updateJoomNullExpressFare'
        self.cur.execute(sql)
        self.con.commit()

    def work(self):
        try:
            begin_date = str(datetime.datetime.today() - datetime.timedelta(days=30))[:10]
            end_date = str(datetime.datetime.today())[:10]
            self.get_data(begin_date, end_date)
            self.update_date()
            self.logger.info('success to update joom express-fare ')
        except Exception as why:
            self.logger.error('failed to update joom express-fare cause of {}'.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    worker = Updater()
    worker.work()
