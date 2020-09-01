#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-03-14 10:07
# Author: turpure

import requests
import json
import os
import sys
from src.services.base_service import CommonService
from configs.config import Config


"""
joom_cralwer采集失败的任务，重新采集。
"""


class Worker(CommonService):

    def __init__(self):
        super().__init__()
        config = Config().config
        self.token = config['ur_center']['token']
        self.base_name = 'mssql'
        self.warehouse = 'mysql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.warehouse_cur = self.base_dao.get_cur(self.warehouse)
        self.warehouse_con = self.base_dao.get_connection(self.warehouse)

    def close(self):
        self.base_dao.close_cur(self.cur)
        self.base_dao.close_cur(self.warehouse_cur)

    def clear(self):
        plat = sys.platform
        if not plat.startswith('win'):
            kill_chrome_cmd = ("ps -ef |grep chrome |awk '{print $2}' | xargs kill -9 && "
                               "ps -ef |grep joom_crawler |grep crawl.py |awk '{print $2}' | xargs kill -9 && "
                               "ps -ef |grep chrome |grep defunct |awk '{print $3}' | xargs kill -9 ")
            os.system(kill_chrome_cmd)
            self.logger.info('success to kill chrome ps')

    def get_tasks(self):
        """
        获取采集状态为【待采集】，且2天之内，10分钟之前的任务
        :return:
        """
        # sql = ("select proId from proCenter.oa_dataMine where platform='joom' and progress in ('待采集', '采集失败')  "
        sql = ("select proId from proCenter.oa_dataMine where platform='joom' and progress in ('待采集')  "
               "and detailStatus='未完善'"
               # " and timestampdiff(day,createTime,now())<=3 and  "
               # "timestampdiff(MINUTE,createTime,now()) >=2"
               )
        self.warehouse_cur.execute(sql)
        ret = self.warehouse_cur.fetchall()
        for row in ret:
            yield row['proId']

    def redo_tasks(self, task_id):
        url = "http://127.0.0.1:8089/v1/oa-data-mine/mine"
        headers = {'content-type': 'application/json', 'Authorization': 'Bearer ' + self.token}
        data = json.dumps({"condition":{"proId": task_id}})
        try:
            ret = requests.post(url,data=data, headers=headers)
            if ret.status_code == 200:
                self.logger.info(f'success to add task {task_id}')
        except Exception as why:
            self.logger.error(f'failed to add task {task_id} cause of {why}')

    def run(self):
        try:
            tasks = self.get_tasks()
            # self.clear()
            for tk in tasks:
                self.redo_tasks(tk)
        except Exception as why:
            self.logger.error(why)
        finally:
            self.close()


if __name__ == '__main__':
    worker = Worker()
    worker.run()


