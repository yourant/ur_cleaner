#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-08-19 17:21
# Author: turpure


from src.services.base_service import BaseService


class Worker(BaseService):

    """
    p_trade 表数据迁移到trade_info里面
    """

    def get_data_from_old_base(self):

        sql = ''



    def put_data_to_new_base(self):

        sql = ''


    def run(self):
        try:

            pass
        except:
            pass

        finally:
            self.close()


if __name__ == '__main__':
    worker = Worker()
    worker.run()
