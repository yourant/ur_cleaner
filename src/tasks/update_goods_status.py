#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-09-16 9:43
# Author: turpure


import pandas as pda
from src.services.base_service import BaseService


class UpdateGoodsStatus(BaseService):

    def run(self):
        try:
            for i in range(1, 9):
                try:
                    step_sql = 'EXEC [guest].[LY_step' + str(i) + '-UpdateGoodsStatus]'
                    self.cur.execute(step_sql)
                    self.con.commit()
                    self.logger.info(f'success to update goods status of step {i}')
                except Exception as why:
                    self.logger.error(f'failed to update goods status of step {i} cause of {why}')
        except Exception as why:
            self.logger.error(f'failed to update goods status  cause of {why}')
        finally:
            self.close()


if __name__ == '__main__':
    worker = UpdateGoodsStatus()
    worker.run()
