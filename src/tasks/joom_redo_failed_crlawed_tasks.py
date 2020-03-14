#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-03-14 10:07
# Author: turpure

import requests
import json
from src.services.base_service import BaseService
from configs.config import Config


"""
joom_cralwer采集失败的任务，重新采集。
"""


class Worker(BaseService):

    def __init__(self):
        config = Config().config
        self.token = config['ur_center']['token']
        super().__init__()

    def get_tasks(self):
        """
        获取采集状态为【待采集】，且2天之内，10分钟之前的任务
        :return:
        """
        sql = ("select proId from proCenter.oa_dataMine where progress='待采集'  and detailStatus='未完善'"
               " and timestampdiff(day,createTime,now())<=30 and  "
               "timestampdiff(MINUTE,createTime,now()) >=5")
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
            for tk in tasks:
                self.redo_tasks(tk)
        except Exception as why:
            self.logger.error(why)
        finally:
            self.close()


if __name__ == '__main__':
    worker = Worker()
    worker.run()


