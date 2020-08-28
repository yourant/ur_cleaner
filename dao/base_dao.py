#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-08-28 13:22
# Author: turpure


from configs.config import Config
from src.services.log import SysLogger
import pymysql

config = Config()


class BaseDao(object):
    """
    database singleton connection
    """

    def __init__(self):
        self.db_con_container = dict()
        self.logger = SysLogger().log

    def connect_database(self, base_name):
        try:
            connection = self.db_con_container.get(base_name, None)
            if not connection:
                connection = self.connect(base_name)
                self.db_con_container[base_name] = connection
            return connection
        except Exception as e:
            self.logger.error('Can not connect to database!', exc_info=True)
            self.db_con_container[base_name] = None
            raise Exception('Can not connect to database!', e)

    def get_connection(self, base_name):
        connection = self.db_con_container.get(base_name, None)
        if not connection:
            connection = self.connect_database(base_name)
        return connection

    def connect(self, base_name):
        try:
            self.logger.info('connect {}...'.format(base_name))

            if base_name == 'mssql':
                import pymssql
                return pymssql.connect(**config.get_config('mssql'))

            if base_name == 'mysql':
                return pymysql.connect(**config.get_config('mysql'))

            if base_name == 'erp':
                data_base_config = config.get_config('erp')
                if data_base_config:
                    return pymysql.connect(**config.get_config('erp'))

            if base_name == 'ibay':
                import psycopg2
                ibay_con = psycopg2.connect(**config.get_config('ibay'))
                ibay_con.set_client_encoding('utf8')
                return ibay_con

        except Exception as why:
            self.logger.error('can not connect {} cause of {}'.format(base_name, why))
            raise why

    def get_cur(self, base_name):
        connection = self.get_connection(base_name)
        cur = None
        if connection:
            if base_name == 'mssql':
                cur = connection.cursor(as_dict=True)

            if base_name == 'mysql':
                cur = connection.cursor(pymysql.cursors.DictCursor)

            if base_name == 'erp':
                cur = connection.cursor(pymysql.cursors.DictCursor)

            if base_name == 'ibay':
                cur = connection.cursor()

        return cur

    def close_cur(self, cur):
        try:
            cur.close()
            self.logger.info(f' close cursor')
        except Exception as why:
            self.logger.error(f'can not close cursor cause of {why}')

    def close(self, base_name):
        connection = self.db_con_container.get(base_name, None)
        try:
            if not connection:
                connection.colse()
        except Exception as why:
            self.logger.error('can not close {} cause of {}'.format(base_name, why))

        finally:
            self.db_con_container[base_name] = None


if __name__ == '__main__':
    do = BaseDao()
    first_mysql = do.get_cur('mysql')
    first_mysql.execute('select * from proCenter.oa_goods limit 1')
    ret = first_mysql.fetchone()
    print(ret)
    do.close_cur(first_mysql)

