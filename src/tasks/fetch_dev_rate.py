#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-11-08 13:04
# Author: turpure

import os
from src.services.base_service import CommonService


class DevRateFetcher(CommonService):
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

    def get_developer(self):
        sql = ("SELECT u.username,CASE WHEN IFNULL(p.department,'')<>'' "
               "THEN p.department ELSE d.department END as depart,"
               "CASE WHEN IFNULL(p.id,'')<>'' THEN p.id ELSE d.id END as departId     "
               "FROM `user` u LEFT JOIN auth_department_child dc ON dc.user_id=u.id "
               "LEFT JOIN auth_department d ON d.id=dc.department_id       "
               "LEFT JOIN auth_department p ON p.id=d.parent      "
               "LEFT JOIN auth_assignment a ON a.user_id=u.id      "
               "WHERE u.`status`=10 AND a.item_name='产品开发'")

        self.warehouse_cur.execute(sql)
        ret = self.warehouse_cur.fetchall()
        for row in ret:
            yield row

    def get_department_rate(self):
        sql = 'select * from Y_Ratemanagement'
        self.cur.execute(sql)
        ret = self.cur.fetchone()
        return {'一部': ret['devRate1'], '五部': ret['devRate5'], '七部': ret['devRate7'], '其他': ret['devRate']}

    def get_developer_rate(self):
        developers = self.get_developer()
        rate_map = self.get_department_rate()
        for dp in developers:
            if dp['depart'] in rate_map:
                yield dp['username'], rate_map[dp['depart']]
            else:
                yield dp['username'], rate_map['其他']

    def push(self, rows):
        try:
            sql = "insert into cache_dev_rate(developer,rate) values (%s,%s) ON DUPLICATE KEY UPDATE rate=values(rate)"
            self.warehouse_cur.executemany(sql, rows)
            self.warehouse_con.commit()
            self.logger.info('success to fetch dev rate')
        except Exception as why:
            self.logger.error(f'fail to push dev rate cause of {why}')

    def work(self):
        try:
            rows = self.get_developer_rate()
            self.push(rows)
        except Exception as why:
            self.logger.error('fail to fetch dev rate cause of {}'.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to fetch dev rate {name}')
        finally:
            self.close()


if __name__ == "__main__":
    worker = DevRateFetcher()
    worker.work()
