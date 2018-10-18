#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-18 11:31
# Author: turpure

import os
import json


class Singleton(object):
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, '_instance'):
            cls._instance = super(Singleton, cls).__new__(cls)
        return cls._instance


class Config(Singleton):
    def __init__(self, env):
        path_list = str.split(os.path.split(os.path.realpath(__file__))[0], os.sep) + [env, 'config.json']
        config_path = os.sep.join(path_list)
        self.config = self._load_config(config_path)

    @staticmethod
    def _load_config(path):
        with open(path, encoding='utf-8') as con:
            return json.load(con)

    def get_config(self, key):
        return self.config[key]


if __name__ == '__main__':
    config = Config('dev')
    print(config.get_config('mssql'))


