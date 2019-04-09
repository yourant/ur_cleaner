#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-04-02 17:16
# Author: turpure

from src.services.base_service import BaseService


class Picker(BaseService):

    def get_tasks(self):
        sql = 'select batchNumber, picker from task_pick where isDone=0'
        try:
            self.warehouse_cur.execute(sql)
            ret = self.warehouse_cur.fetchall()
            for row in ret:
                yield row
        except Exception as why:
            self.logger.error('fail to get picking task')
            yield

    def do_task(self, row):
        task_sql = "update p_trade set packingMen=%s where batchNum=%s"
        status_sql = 'update task_pick set updatedTime=now(),isDone=1 where batchNumber=%s'
        try:
            self.cur.execute(task_sql, (row['picker'], row['batchNumber']))
            self.warehouse_cur.execute(status_sql, (row['batchNumber']))
            self.logger.info('{} picked {}'.format(row['picker'], row['batchNumber']))
            self.warehouse_con.commit()
            self.con.commit()
        except Exception as why:
            self.logger.error('{} failed to pick {} cause of {}'.format(row['picker'], row['batchNumber'], why))

    def work(self):
        try:
            for row in self.get_tasks():
                self.do_task(row)
        except Exception as why:
            self.logger.error('fail to do task cause of {}'.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    picker = Picker()
    picker.work()

