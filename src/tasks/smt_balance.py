#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure


import os
import re
import datetime
from src.services.base_service import CommonService
from sync.tupianku_image_delete.image_server import BaseSpider
import requests
import asyncio
import aiohttp

from pymongo import MongoClient


class Worker(BaseSpider):
    """
    worker template
    """

    def __init__(self):
        super().__init__()
        self.token = None

    async def login(self):
        base_url = 'https://passport.aliexpress.com/newlogin/login.do?fromSite=13&appName=aeseller'
        form_data = {
            'loginId': 'bronzeq@163.com',
            'password2': 'nty@8j2af5n'
        }
        res = await self.session.post(base_url, data=form_data, proxy=self.proxy_url)
        print(await res.text())
        self.logger.info(f'success')

    async def get_balance(self, sema):
        async with sema:
            try:
                url = 'https://global.alipay.com/merchant/bizportal/balance/new/account-list?_route=US'
                url = 'https://global.alipay.com/merchant/merchantservice/api/merchantservice/v2/bizfund/queryBizFundAssets.json?ctoken=B81U5UkFeTujvvJp&_route=US'

                try:
                    response = await self.session.get(url)
                    ret = await response.json()
                    print(ret)


                except Exception as why:
                    # await self.login()
                    self.logger.error(f'failed to find images of {123} cause of {type(why)}')
            except Exception as why:
                await self.login()
                self.logger.error(f'error while delete image of goodsCode "{123}" cause of {why}')

    async def start(self, sema):

        await self.login()
        await self.get_balance(sema)
        print(123)
        await self.session.close()

    def run(self):
        loop = asyncio.get_event_loop()
        sema = asyncio.Semaphore(30)
        try:
            loop.run_until_complete(self.start(sema))
        except Exception as why:
            self.logger.error(why)
        finally:
            # self.logger.error(f'fail 3')
            self.close()

    def work(self):
        # try:
            self.run()
        # except Exception as why:
        #     self.logger.error(f'fail 1')
        # finally:
        #     self.logger.error(f'fail 2')


if __name__ == "__main__":
    worker = Worker()
    worker.work()


