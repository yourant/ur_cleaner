#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-11-08 17:02
# Author: turpure


from abc import abstractmethod
from src.services.base_service import BaseService
from configs.config import Config
import asyncio
import aiohttp

class BaseSpider(BaseService):

    def __init__(self,tupianku_name=2):
        super().__init__()
        config = Config()
        self.tupianku_name = tupianku_name
        self.tupianku_info = config.get_config(f'tupianku{tupianku_name}')
        self.proxy_url = "http://127.0.0.1:1080"
        self.proxy_url = None
        self.session = aiohttp.ClientSession()

    async def login(self):

        base_url = 'https://www.tupianku.com/login'
        form_data = {
            'action':'login',
            'task':'login',
            'return':'',
            'remember':1,
            'username': self.tupianku_info['username'],
            'password': self.tupianku_info['password']
        }
        await self.session.post(base_url, data=form_data,proxy=self.proxy_url)
        self.logger.info(f'success to login tupianku{self.tupianku_name}')


    async def search_image(self, goodsCode):
        base_url = 'https://www.tupianku.com/myfiles'
        form_data = {
            'action':'search',
            'current_folder_id':self.tupianku_info['folder_id'],
            'move_to_folder_id':0,
            'sort':'date_desc',
            'fl_per_page': 800,
            'keyword': goodsCode
        }
        ret = await self.session.post(base_url, data=form_data, proxy=self.proxy_url)
        ret = self.get_image_ids(await ret.text())
        self.logger.info(f'find {len(ret)} images of {goodsCode}')
        asyncio.sleep(0.4)
        return ret

    @abstractmethod
    async def delete_image(self, goods_code, image_ids=[],):
        base_url = 'https://www.tupianku.com/myfiles'
        form_data = {
            'action': 'delete',
            'current_folder_id': self.tupianku_info['folder_id'],
            'move_to_folder_id': 0,
            'sort': 'date_desc',
            'fl_per_page': 800,
            'keyword': '',
            'file_ids[]': image_ids
        }
        ret = await self.session.post(base_url, data=form_data, proxy=self.proxy_url)
        self.logger.info(f'success to delete images of {goods_code} ')
        return ret

    @staticmethod
    def get_image_ids(html):
        image_ids = []
        num = html.count('mf_addfile(')
        for i in range(num +1):
            index = html.find('mf_addfile(')
            if index > 0:
                image_ids.append(html[index + 11:index + 19])
                html = html[index + 20:]
        return image_ids

    async def run(self):
        try:
            goods = await self.get_goods()
            self.logger.info(goods)
            for k in goods:
                print(k['goodsCode'])
                await self.deal(k['goodsCode'])
                # 保存已经删除图片的goodsCode
                await self.save(k['goodsCode'])
        except Exception as why:
            self.logger.error(f'fail to delete image cause of {why} in async way')
        finally:
            self.close()