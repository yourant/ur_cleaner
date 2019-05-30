#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-05-30 9:14
# Author: turpure

import requests
import datetime


def get_sentence():
    """
    金山api
    :return:
    """
    today = str(datetime.datetime.now())[:10]
    # date = str(int(today[0:4]) - ) + today[4:]
    base_url = 'http://open.iciba.com/dsapi/?date={}'.format(today)
    try:
        res = requests.get(base_url)
        ret = res.json()
        return ret['content']
    except:
        return 'this is a beautiful day'


def get_quote():
    """
    天行api
    :return:
    """
    key = '186b6eeac08d0529bc3d073f50d776f3'
    base_url = 'http://api.tianapi.com/txapi/ensentence/?key={}'.format(key)
    try:
        res = requests.get(base_url)
        ret = res.json()
        return ret['newslist'][0]['en']
    except:
        return 'this is a wonderful day'


if __name__ == "__main__":
    get_quote()
