#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-18 14:44
# Author: turpure


from configs.config import Config
from src.services.log import SysLogger

config = Config()


class Mssql(object):
    """
    sqlServer singleton connection
    """
    connect = None

    def __init__(self):
        if not self.connect:
            SysLogger().log.info('not existing db...')
            self.connect = self._connect()

    @staticmethod
    def _connect():
        try:
            SysLogger().log.info('connect db...')
            import pymssql
            return pymssql.connect(**config.get_config('mssql'))

        except Exception as why:
            SysLogger().log.info('can not connect db cause of %s' % why)
            import pymysql
            return None

    @property
    def connection(self):
        return self.connect

    def close(self):
        SysLogger().log.info('close db...')
        self.connect.close()


class DataBase(object):
    """
    database singleton connection
    """
    connect = None

    def __init__(self, base_name):
        self.base_name = base_name
        if not self.connect:
            SysLogger().log.info('not existing db...')
            self.connect = self._connect()

    def _connect(self):
        try:
            SysLogger().log.info('connect db...')

            if self.base_name == 'mssql':

                import pymssql
                return pymssql.connect(**config.get_config('mssql'))

            if self.base_name == 'mysql':

                import pymysql
                return pymysql.connect(**config.get_config('mysql'))

        except Exception as why:
            SysLogger().log.info('can not connect db cause of %s' % why)
            return None

    @property
    def connection(self):
        return self.connect

    def close(self):
        SysLogger().log.info('close db...')
        self.connect.close()


if __name__ == '__main__':
    import pymysql
    con = DataBase('mysql')
    cur = con.connection.cursor(pymysql.cursors.DictCursor)
    cur.execute('select * from requirement')
    ret = cur.fetchall()
    for row in ret:
        print(row)
    con.close()



