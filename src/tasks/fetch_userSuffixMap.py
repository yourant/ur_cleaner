#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-11-08 13:04
# Author: turpure

import os
from src.services.base_service import CommonService


class UserSuffixMapFetcher(CommonService):
    """
    fetch suffix profit from erp day by day
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

    def get_user(self):
        sql = ("SELECT username as userName,ats.store FROM `auth_store` ats LEFT JOIN auth_store_child AS atc "
               "ON atc.store_id = ats.id LEFT JOIN `user` AS u ON u.id = atc.user_id "
               "LEFT JOIN auth_department_child AS adpc ON adpc.user_id = u.id "
               "LEFT JOIN auth_department AS adp ON adpc.department_id = adp.id "
               "LEFT JOIN auth_department AS adpp ON adp.parent = adpp.id")

        self.warehouse_cur.execute(sql)
        ret = self.warehouse_cur.fetchall()
        for row in ret:
            yield row['userName'], row['store']

    def push(self, rows):
        try:
            clear_table = 'truncate table oauth_userSuffixMap '
            self.cur.execute(clear_table)
            for rw in rows:
                try:
                    sql = "insert into oauth_userSuffixMap(username,suffix) values (%s,%s)"
                    self.cur.execute(sql, rw)
                except Exception as why:
                    sql = "update oauth_userSuffixMap set username=%s where suffix = %s "
                    self.cur.execute(sql, rw)
            self.con.commit()
            self.logger.info('success to fetch user suffix map')
        except Exception as why:
            self.logger.error(f'fail to push fetch user suffix map cause of {why}')

    def work(self):
        try:
            rows = self.get_user()
            self.push(rows)
        except Exception as why:
            self.logger.error('fail to fetch user suffix map cause of {}'.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to fetch user suffix map {name}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = UserSuffixMapFetcher()
    worker.work()
