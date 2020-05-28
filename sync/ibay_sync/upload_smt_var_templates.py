#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-05-22 14:49
# Author: henry

import time
import asyncio
import datetime
from bs4 import BeautifulSoup
import os
import re
from sync.ibay_sync.smt_ibay_server import login_session
from src.services.base_service import BaseService




class Upload(BaseService):
    """
    Export goods info to excel file
    """
    def __init__(self):
        super().__init__()
        self.path = '../../runtime/smt/'
        self.session = login_session()
        self.upload_url = 'http://139.196.109.214/index.php/import/aliexpressimportxlsvar'




    def upload(self, path):
        try:
            with open(path, 'rb') as files:

                data = {'mubanxls': ('report.xls', files, 'application/vnd.ms-excel')}
                res = self.session.post(self.upload_url, files=data)
                soup = BeautifulSoup(res.content, features='html.parser')
                # print(soup)

                html = soup.find(text=re.compile("导入成功"))
            if html:
                    mubanId = re.findall(r'\d+', html)
                    # print(mubanId)
                    if mubanId :
                        self.remark_data(path, mubanId[0])
            else:
                self.logger.error('{} is failed to upload goods var'.format(path))
        except Exception as why:
            self.logger.error('{} is failed to upload cause of {}'.format(path, why))
            os.remove(path)  # 删除excel文件


    def remark_data(self, path, muban_id):
        sql = ''
        params = ()
        list = []
        try:
            now = str(datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S'))
            if path.find('SMT2') != -1:
                list = path.split('.')
                sql = "update proCenter.oa_smtImportToIbayLog set completeDate2=%s,status2=%s where mubanId=%s;"
                params = (now, 1, list[5])

            self.warehouse_cur.execute(sql, params)
            self.warehouse_con.commit()


            self.logger.error('Success to remark data')
        except Exception as why:
            self.logger.error('Failed to remark data cause of {}'.format(why))



    async def run(self):
        try:
            # 获取所有表格
            for input_file in os.listdir(self.path):
                path = self.path + input_file
                self.upload(path)  # 上传表格
                os.remove(path)

        except Exception as why:
            self.logger.error('Failed to upload goods var templates cause of {}'.format(why))
        finally:
            self.close()



if __name__ == '__main__':

    start = time.time()
    export = Upload()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(export.run())
    end = time.time()
    date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
    print(date + f' it takes {end - start} seconds')





