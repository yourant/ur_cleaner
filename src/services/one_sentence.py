#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-05-30 9:14
# Author: turpure

import requests
import datetime


def get_sentence():
    today = str(datetime.datetime.now())[:10]
    date = str(int(today[0:4]) - 7) + today[4:]
    print(date)
    base_url = 'http://open.iciba.com/dsapi/?date={}'.format(date)
    try:
        res = requests.get(base_url)
        ret = res.json()
        return ret['content']
    except:
        return 'this is a beautiful day'


if __name__ == "__main__":
    print(get_sentence())
