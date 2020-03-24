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

                image_ids.append(html[index + 11:index + 20].split(','[0]))
                html = html[index + 21:]
        return image_ids

