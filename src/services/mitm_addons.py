#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-05-18 17:34
# Author: turpure


import mitmproxy.http
from db import DataBase


class Counter:
    def __init__(self):
        self.num = 0

    def request(self, flow: mitmproxy.http.HTTPFlow):
        headers = flow.request.headers
        url = flow.request.url
        for row in headers.items(True):
            if row[0] == 'x-api-token' and url.startswith('https://www.joom.com/tokens/init'):
                self.save(row)
                break

    def save(self, row):
        con = DataBase('mysql')
        try:
            cur = con.connection.cursor()
            sql = 'update urTools.sys_joom_token set token=%s,updateTime=now() where id=1'
            cur.execute(sql, row[1])
            con.connection.commit()
            print('saving {}'.format(row))
        except Exception as why:
            print(why)
        finally:
            con.close()




addons = [
    Counter()
]
