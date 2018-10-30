#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-30 15:07
# Author: turpure

from src.services.base_service import BaseService


class Fetcher(BaseService):
    """
    fetch suffix sales from erp and put them into data warehouse
    """

    def __init__(self):
        super().__init__()

    def fetch(self, date_flag, begin_date, end_date):
        sql = 'oauth_saleTrendy @dateFlag=%s, @beginDate=%s, @endDate=%s'
        self.cur.execute(sql, (date_flag, begin_date, end_date))
        ret = self.cur.fetchall()
        return ret

    def push(self, row):
        sql = 'insert into cache_suffixSales() values(%s,%s,%s, %s)'
