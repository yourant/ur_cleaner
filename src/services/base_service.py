#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-30 15:09
# Author: turpure

from dao.base_dao import BaseDao


class BaseService(object):
    """
    wrap log and db service
    """
    base_dao = BaseDao()

    def __init__(self):
        self.logger = self.base_dao.logger


