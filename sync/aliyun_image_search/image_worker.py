#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-11-26 11:08
# Author: turpure


from sync.aliyun_image_search.base_request import BaseRequest
from src.services.base_service import BaseService


class Worker(BaseService):

    def __init__(self):
        super().__init__()
        self.request = BaseRequest()

    def get_image_url(self):
        sql = "select BmpFileName as img  from  b_goods where isnull(BmpFileName, '') != '' "
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row['img']

    def run(self):
        try:
            images = self.get_image_url()
            for img in images:
                self.request.add(img)

        except Exception as why:
            self.logger.error(f'fail to run image-worker cause of {why}')
        finally:
            self.close()


if __name__ == '__main__':
    worker = Worker()
    worker.run()


