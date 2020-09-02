#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-20 16:10
# Author: turpure

import json
import hashlib
import datetime
from src.services import log
from configs.config import Config


class Wyt(object):
    """
    get refresh_token from https://open.1688.com/api/apiTool.htm?spm=a260s.8209109.0.0.lQxRzL
    """
    def __init__(self):
        self.logger = log.SysLogger().log
        self.platform = Config().get_config('wyt')['platform']
        self.app_key = Config().get_config('wyt')['app_key']
        self.token = Config().get_config('wyt')['token']
        self.client_id = Config().get_config('wyt')['client_id']
        self.client_secret = Config().get_config('wyt')['client_secret']


    def get_signature(self, token, params):
        ordered_par_dict = sorted(key + params[key] for key in params)
        par_string = ''.join(ordered_par_dict)
        raw_string = token + par_string + token
        signature = hashlib.md5(raw_string.encode("utf8")).hexdigest().upper()
        return signature

    def get_request_par(self, data, action, version = '1.0'):
        today = str(datetime.datetime.today())[:19]
        params = {
            'app_key': self.app_key,
            'platform': self.platform,
            'action': action,
            'data': json.dumps(data),
            'format': 'json',
            'timestamp': today,
            'sign_method': 'md5',
            'version': version
        }
        sign = self.get_signature(self.token, params)
        client_sign = self.get_signature(self.client_secret, params)
        params['sign'] = sign
        params['client_sign'] = client_sign
        params['client_id'] = self.client_id
        params['language'] = 'zh_CN'
        params['data'] = data

        return params


if __name__ == "__main__":
    ali = Wyt()
    print(ali.get_request_url('898565793088682293'))
