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
            'password2': 'ba9f68bdee1681dd906ea8927942c46168b2e8416bfb49a4fdfef3463f8040d12dd372e6c778eff44a8ed20a5e54650b7b61eaae18f80ec93e41fe9a19aea5b733c0bf5f1e75f528c4f8f43a4c5d4b82e617cf8faac90e7499028482638b2594227fe18c67ab0d41208b60242384c5605d95c1c221e779160fbb102713e89c73'
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


