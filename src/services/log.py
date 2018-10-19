#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-19 8:47
# Author: turpure

import logging
from configs.config import Config, Singleton

config = Config()


class SysLogger(Singleton):
    """
    singleton logger service
    """
    logger = None

    def __init__(self):
        if not self.logger:
            self.logger = self._logger()

    @staticmethod
    def _logger():
        setting = config.get_config('logger')
        logger = logging.getLogger(setting['name'])
        logger.setLevel(setting['level'])
        formatter = logging.Formatter(setting['formatter'])
        console = logging.StreamHandler()
        console.setFormatter(formatter)
        file_handler = logging.FileHandler(setting['path'])
        file_handler.setFormatter(formatter)
        logger.addHandler(console)
        logger.addHandler(file_handler)
        return logger

    @property
    def log(self):
        return self.logger






