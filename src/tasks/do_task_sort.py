#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-04-02 17:16
# Author: turpure

from src.services.base_service import BaseService


class Sorter(BaseService):

    def get_tasks(self):
        sql = 'select batchNumber, picker from task_sort where isDone=0'
        try:
            self.warehouse_cur.execute(sql)
            ret = self.warehouse_cur.fetchall()
            for row in ret:
                yield row
        except Exception as why:
            self.logger.error('fail to get sorting task cause of {}'.format(why))
            yield

    def get_trades(self, row):
        trades_to_update = ("select tradeNid, pickupNo as batchNumber "
                            "from P_TradePickup(nolock)  where 1=1  and PickupNo = '{}' ")
        try:
            self.cur.execute(trades_to_update.format(row['batchNumber']))
            ret = self.cur.fetchall()
            for out in ret:
                out['picker'] = row['picker']
                yield out

        except Exception as why:
            self.logger.error('failed to get trades cause of {}'.format(why))

    def do_task(self, row):
        task_sql = "update p_trade set OrigPackingMen=%s where nid=%s"
        try:
            self.cur.execute(task_sql, (row['picker'], row['tradeNid']))
            self.con.commit()
        except Exception as why:
            self.logger.error('{} failed to sort {} cause of {}'.format(row['tradeNid'], row['batchNumber'], why))

    def after_task(self, row):
        status_sql = 'update task_sort set updatedTime=now(),isDone=1 where batchNumber=%s'
        try:
            self.warehouse_cur.execute(status_sql, (row['batchNumber']))
            self.warehouse_con.commit()
            self.logger.info('{} sorted {}'.format(row['picker'], row['batchNumber']))
        except Exception as why:
            self.logger.error('failed to finish task {} cause of {}'.format(row['batchNumber'], why))

    def work(self):
        try:
            for row in self.get_tasks():
                for trades in self.get_trades(row):
                    self.do_task(trades)
                self.after_task(row)
        except Exception as why:
            self.logger.error('fail to do task cause of {}'.format(why))
        finally:
            self.close()


if __name__ == "__main__":
    picker = Sorter()
    picker.work()
