#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-09-16 9:43
# Author: turpure


from src.services.base_service import CommonService


class UpdateGoodsStatus(CommonService):

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

    def close(self):
        self.base_dao.close_cur(self.cur)

    def run(self):
        try:
            for i in range(1, 8):
                try:
                    step_sql = 'EXEC [dbo].[LY_step' + str(i) + '-UpdateGoodsStatus_New]'
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
