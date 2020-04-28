#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-01-04 14:49
# Author: turpure

import datetime
import time
import asyncio
from multiprocessing import Pool, Lock
import os
from sync.ibay_sync.smt_ibay_server import  generate
from src.services.base_service import BaseService

from sync.ibay_sync.import_smt_model import Uploader



class Export(BaseService):
    """
    Export goods info to excel file
    """
    def __init__(self):
        super().__init__()
        self.path = '../../runtime/'
        self.multiple = {
            "mubanid": "",
            "sku": "",
            "quantity": "",
            "price": "",
            "pic_url": "",
            "skuimage": 'Color',
            "varition1": "Color:Black(黑色)",
            "name1": "",
            "varition2": "Size:S(小号)",
            "name2": "",
            "varition3": "",
            "name3": "",
            "varition4": "",
            "name4": "",
            "varition5": "",
            "name5":""
        }


    # 获取产品多属性 数据
    def get_var_data(self):
        sql = 'select  * from proCenter.oa_smtImportToIbayLog where status1=1 and status2=0'
        self.warehouse_cur.execute(sql)
        self.warehouse_con.commit()
        ret = self.warehouse_cur.fetchall()
        return ret


    async def deal_var_data(self, data):
        for item in data:
            rows = []
            smtSql = ('select  * from proCenter.oa_smtGoodsSku ae inner join proCenter.oa_goodsinfo g on g.id=ae.infoId '  +
                  ' where goodsCode = %s or sku = %s;')
            self.warehouse_cur.execute(smtSql, (item['SKU'], item['SKU']))
            smtQuery = self.warehouse_cur.fetchall()
            print(smtQuery)
            if smtQuery:
                for row in smtQuery:
                    res = self.multiple
                    res['mubanid'] = item['mubanId']
                    res['sku'] = row['sku']
                    res['quantity'] = row['quantity']
                    res['price'] = row['price']
                    res['pic_url'] = row['pic_url']
                    rows.append(res)

            print(rows)
            now = str(datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
            file_name = self.path + 'SMT2' + '.' + str(rows[0]['mubanid']) + '.'  + now + '.xls'
            generate(rows, file_name)  # 导出单属性数据




    async def work(self):
        l = Lock()
        p = Pool(16, initializer=init, initargs=(l,))
        paths = []
        for input_file in os.listdir(self.path):
            paths.append(self.path + input_file)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            # p.map_async(work, paths)
        p.map_async(upload_multiple, paths)
        p.close()
        p.join()



    async def run(self):
        try:
            # 获取多属性数据
            var_list = self.get_var_data()  # 获取数据
            if var_list:
                await self.deal_var_data(var_list)  # 处理数据 并导出表格

            await self.work()  # 导入多属性数据，记录结果
            print('success to import var data!')
        except Exception as why:
            print('failed to import var data cause of {}'.format(why))


def init(l):
    global lock
    lock = l



def upload_multiple(path):
    uploader = Uploader(path)
    uploader.run('multiple')




if __name__ == '__main__':

    start = time.time()
    export = Export()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(export.run())
    end = time.time()
    print(f'it takes {end - start} seconds')





