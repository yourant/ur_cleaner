#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-18 14:44
# Author: turpure

import pymssql

from configs.config import Config, Singleton
from src.services.log import SysLogger

config = Config()


class Mssql(Singleton):
    """
    sqlServer singleton connection
    """
    def __init__(self):
        self.connect = self._connect()

    @staticmethod
    def _connect():
        try:
            SysLogger().log.info('connect db...')
            return pymssql.connect(**config.get_config('mssql'))

        except Exception as why:
            SysLogger().log.info('can not connect db cause of %s' % why)
            return None

    @property
    def connection(self):
        return self.connect

    def close(self):
        self.connect.close()


if __name__ == '__main__':
    con = Mssql()
    cur = con.connection.cursor()
    cur.execute('select top 10 * from oa_goods')
    ret = cur.fetchall()
    for row in ret:
        print(ret)



