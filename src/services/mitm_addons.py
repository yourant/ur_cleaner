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
        if url.startswith('https://www.joom.com/tokens/init'):
            try:
                headers = self.pares_headers(headers)
                row = dict()
                print(headers)
                row['api_token'] = headers['x-api-token']
                row['access_token'] = 'Bearer ' + self.parse_cookies(headers['cookie'])['accesstoken']
                self.save(row)
            except Exception as why:
                print(why)

        # for row in headers.items(True):
        #     if row[0] == 'x-api-token' and url.startswith('https://www.joom.com/tokens/init'):
        #         self.save(row)
        #         break

    @staticmethod
    def parse_cookies(cookies):
        return {item.split('=')[0]: item.split('=')[1] for item in cookies.split('; ')}

    @staticmethod
    def pares_headers(headers):
        headers = headers.items(True)
        return {ele[0]: ele[1] for ele in headers}

    @staticmethod
    def save(row):
        con = DataBase('mysql')
        try:
            cur = con.connection.cursor()
            sql = 'update urTools.sys_joom_token set token=%s, bearerToken=%s,updateTime=now() where id=1'
            cur.execute(sql, (row['api_token'], row['access_token']))
            con.connection.commit()
            print('saving {}'.format(row))
        except Exception as why:
            print(why)
        finally:
            con.close()

addons = [
    Counter()
]
