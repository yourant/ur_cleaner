#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-10-19 14:23
# Author: turpure


import requests
import json
from src.services.base_service import BaseService


class Worker(BaseService):

    def get_rule(self):
        rule = {}
        return rule

    @staticmethod
    def log_in():
        base_url = 'http://www.haiyingshuju.com/auth/login'
        form_data = {
            'username': 'IWj+UaaCx4WpLQ01XoyaNw==',
            'password': '6SJ2Lmmt0BKKiSWhQipe+Q=='
        }
        ret = requests.post(base_url, data=form_data)
        return ret.headers['token']

    def get_product(self):
        url = "http://www.haiyingshuju.com/ebay/newProduct/list"
        token = self.log_in()
        payload = {'cids':'', 'index':1, 'title':'','itemId':'','soldEnd':'','country':1,'visitEnd':'','priceEnd':'','soldStart':'','titleType':1,'sort':'DESC','pageSize':20,'priceStart':'','visitStart':'','marketplace':[],'popularStatus':'','sellerOrStore':'','storeLocation':[],'salesThreeDayFlag':'','orderColumn':'last_modi_time','listedTime':['2019-10-19','2019-10-18','2019-10-17'],'itemLocation':['CH']}
        headers = {
            'Accept': "application/json, text/plain, */*",
            'Accept-Encoding': "gzip, deflate",
            'Accept-Language': "zh-CN,zh;q=0.9,en;q=0.8",
            'Connection': "keep-alive",
            # 'Content-Length': "390",
            'Content-Type': "application/json",
            # 'Cookie': "Hm_lvt_03a80b70183e649c063d5ee13290d51b=1571296557,1571302643,1571363001,1571466012; JSESSIONID=998FFB5792FD290DF29063EF1D9F057E; token=Bearer eyJhbGciOiJIUzUxMiJ9.eyJ1aWQiOjM3MzM5LCJzdWIiOiJ5b3VyYW4wMDEiLCJjcmVhdGVkRGF0ZSI6MTU3MTQ2NjQzNTE3MiwiaXNzIjoiaHlzaiIsImV4cCI6MTU3MTQ3MzYzNSwidXVpZCI6IjlmMGU3YjM3LWFhZTItNDE1NS1hMDhiLTU2N2U0YzgxMjZjMCIsImlhdCI6MTU3MTQ2NjQzNX0.hFZbUvMb2N1Tk6BZ-G6qHgUu87s_3geenrR8aNLE-Zt7MtusfEuzxB423PSRzVrA4QXdVZEIO_r1DHdm_0SBCA; Hm_lpvt_03a80b70183e649c063d5ee13290d51b=1571466435",
            'Host': "www.haiyingshuju.com",
            'Origin': "http://www.haiyingshuju.com",
            'Referer': "http://www.haiyingshuju.com/ebay/index.html",
            'token': token,
            'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.120 Safari/537.36",
            'Cache-Control': "no-cache",
            'cache-control': "no-cache"
        }

        response = requests.post(url, data=json.dumps(payload), headers=headers)

        print(response.json()['data'])

    def run(self):
        try:
            self.get_product()
            # self.log_in()
        except Exception as why:
            self.logger.error(f'fail to get ebay products cause of {why}')
        finally:
            self.close()


if __name__ == '__main__':
    worker = Worker()
    worker.run()
