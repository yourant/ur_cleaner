#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-18 14:44
# Author: turpure


from configs.config import Config
from src.services.log import SysLogger

config = Config()


class DataBase(object):
    """
    database singleton connection
    """
    connect = None
    used_count = 0

    def __init__(self, base_name):
        self.base_name = base_name
        if not self.connect:
            self.used_count += 1
            SysLogger().log.info('not existing {} connection...'.format(self.base_name))
            self.connect = self._connect()

    def _connect(self):
        try:
            SysLogger().log.info('connect {}...'.format(self.base_name))

            if self.base_name == 'mssql':

                import pymssql
                return pymssql.connect(**config.get_config('mssql'))

            if self.base_name == 'mysql':

                import pymysql
                return pymysql.connect(**config.get_config('mysql'))

            if self.base_name == 'erp':

                import pymysql
                data_base_config = config.get_config('erp')
                if data_base_config:
                    return pymysql.connect(**config.get_config('erp'))

            if self.base_name == 'ibay':
                data_base_config = config.get_config('ibay')
                if data_base_config:
                    import psycopg2
                    return psycopg2.connect(**config.get_config('ibay'))

        except Exception as why:
            SysLogger().log.info('can not connect {} cause of {}'.format(self.base_name, why))
            return None

    @property
    def connection(self):
        return self.connect

    def close(self):
        try:
            if self.used_count == 1:
                SysLogger().log.info('close {}...'.format(self.base_name))
                self.connect.close()
            if self.used_count > 1:
                SysLogger().log.info('close {} by decreasing one connection'.format(self.base_name))
                self.used_count -= 1
        except Exception as why:
            SysLogger().log.error('fail to close connection cause of {}'.format(why))


if __name__ == '__main__':
    import psycopg2
    import psycopg2.extras
    con = DataBase('ibay')
    cur = con.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute('select * from ebay_item limit 10')
    ret = cur.fetchall()
    for row in ret:
        print(row)
    con.close()



