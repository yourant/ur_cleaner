#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-20 16:10
# Author: turpure

import json
import hmac
from hashlib import sha1
import requests
from src.services import log
from configs.config import Config, Singleton


class Ali(Singleton):
    """
    get refresh_token from https://open.1688.com/api/apiTool.htm?spm=a260s.8209109.0.0.lQxRzL
    """
    def __init__(self):
        self.logger = log.SysLogger().log
        self.api_name = Config().get_config('ali')['api_name']
        self.app_key = Config().get_config('ali')['app_key']
        self.app_secret_key = Config().get_config('ali')['app_secret_key']
        self.refresh_token = Config().get_config('ali')['refresh_token']
        self.token = self._get_access_token()

    def _get_access_token(self):
        base_url = 'https://gw.open.1688.com/openapi/param2/1/system.oauth2/getToken/%s' % self.app_key
        post_data = {
            'grant_type': 'refresh_token',
            'client_id': self.app_key,
            'client_secret': self.app_secret_key,
            'refresh_token': self.refresh_token,
        }

        try:
            res = requests.post(base_url, data=post_data)
            body = res.content
            ret = json.loads(body)
            return ret['access_token']

        except Exception as e:
            self.logger.error('%s:error while getting access token' % e)

    def get_signature(self, order_id):
        url_path = 'param2/1/com.alibaba.trade/%s/%s' % (self.api_name, self.app_key)
        token = self.token
        signature_par_dict = {
            'webSite': '1688',
            'access_token': token,
            'orderId': order_id
        }
        ordered_par_dict = sorted(key + signature_par_dict[key] for key in signature_par_dict)
        par_string = ''.join(ordered_par_dict)
        raw_string = url_path + par_string
        signature = hmac.new(bytes(self.app_secret_key, 'utf-8'),
                             bytes(raw_string, 'utf-8'),
                             sha1).hexdigest().upper()
        return signature

    def get_request_url(self, order_id):
        token = self.token
        signature = self.get_signature(order_id)
        head = [
            'http://gw.open.1688.com:80/openapi/param2/1/com.alibaba.trade',
            self.api_name,
            self.app_key
        ]
        url_head = '/'.join(head)
        para_dict = {
            'webSite': '1688',
            'orderId': order_id,
            '_aop_signature': signature,
            'access_token': token
        }
        parameter = [key + "=" + para_dict[key] for key in para_dict]
        url_tail = "&".join(parameter)
        base_url = url_head + "?" + url_tail
        return base_url


if __name__ == "__main__":
    ali = Ali()
    print(ali.get_request_url('264849699445768662'))
