#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-11-08 17:02
# Author: turpure


from abc import abstractmethod
from src.services.base_service import BaseService
from configs.config import Config


class BaseSpider(BaseService):

    def __init__(self):
        super().__init__()
        config = Config()
        self.tupianku_info = config.get_config('tupianku')

    @abstractmethod
    async def get_goods(self):
        pass

    async def log_in(self, session):
        base_url = 'https://www.tupianku.com/login'
        form_data = {
            'action':'login',
            'task':'login',
            'return':'',
            'remember':1,
            'username': self.tupianku_info['username'],
            'password': self.tupianku_info['password']
        }
        ret = await session.post(base_url, data=form_data)
        return ret


    async def search_image(self, session, goodsCode):
        base_url = 'https://www.tupianku.com/myfiles'
        form_data = {
            'action':'search',
            'current_folder_id':34708,
            'move_to_folder_id':0,
            'sort':'date_desc',
            'fl_per_page': 800,
            'keyword': goodsCode
        }
        ret = await session.post(base_url, data=form_data)
        return ret



    @abstractmethod
    async def delete_image(self, session, image_ids):
        base_url = 'https://www.tupianku.com/myfiles'


        form_data = {
            'action': 'delete',
            'current_folder_id': 34708,
            'move_to_folder_id': 0,
            'sort': 'date_desc',
            'fl_per_page': 800,
            'keyword': '',
            'file_ids[]':image_ids
        }
        ret = await session.post(base_url, data=form_data)
        # print(ret)
        # return ret



    @abstractmethod
    async def deal(self, goodsCode):
        pass



    @staticmethod
    def get_image_ids(html):
        image_ids = []
        num = html.count('mf_addfile(')
        for i in range(num +1):
            index = html.find('mf_addfile(')
            if(index > 0):
                str = html[index + 11:index + 21]
                image_ids.append(str.split(',')[0])
                html = html[index + 20:]
        return image_ids



    @abstractmethod
    async def save(self, goodsCode):
        pass



    async def run(self):
        try:
            goods = await self.get_goods()
            self.logger.info(goods)
            for k in goods:
                # print(k['goodsCode'])
                await self.deal(k['goodsCode'])
                # 保存已经删除图片的goodsCode
                await self.save(k['goodsCode'])
                self.logger.info(f'success to delete image of goodsCode "{k["goodsCode"]}" in async way')
        except Exception as why:
            self.logger.error(f'fail to delete image cause of {why} in async way')
        finally:
            self.close()






