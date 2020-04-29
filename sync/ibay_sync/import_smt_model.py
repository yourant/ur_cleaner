#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-01-04 14:49
# Author: turpure

import time
import asyncio
import datetime
from bs4 import BeautifulSoup
from multiprocessing import Pool, Lock
import os
import re
import pandas as pd
from sync.ibay_sync.smt_ibay_server import login_session, generate
from src.services.base_service import BaseService

from functools import partial




class Uploader(BaseService):
    """
    import excel file to ibay
    """

    def __init__(self, file_names):
        super().__init__()
        # self.path = '../../runtime/'
        self.path = file_names
        self.session = login_session()
        self.upload_url = {
            'single': 'http://139.196.109.214/index.php/import/aliexpressimportxls',
            'multiple': 'http://139.196.109.214/index.php/import/aliexpressimportxlsvar',
        }


    def upload(self, flag):
        """
        import the file into server via session
        :return:
        """

        try:
            with open(self.path, 'rb') as files:
                # xl = pd.read_excel(self.path)
                # print(xl.size)
                # if xl.size == 0:
                #     return True

                data = {'mubanxls': ('report.xls', files, 'application/vnd.ms-excel')}
                res = self.session.post(self.upload_url[flag], files=data)
                soup = BeautifulSoup(res.content, features='html.parser')
                # print(soup)
            try:
                html = soup.find(text=re.compile("导入成功"))
                mubanId = re.findall(r'\d+', html)
                if mubanId :
                    # print(mubanId)
                    return mubanId[0]
            except AttributeError as why:
                with lock:
                    xl = pd.read_excel(self.path)
                    pattern = re.compile(r'第*([0-9]*)?行')
                    row_numbers = []
                    for ele in soup(text=pattern):
                        row_num = re.findall(r'第*([0-9]*)?行', str(ele))[0]
                        print('{} delete {}'.format(str(datetime.datetime.now()),
                                                    xl.iloc(int(row_num) - 2)[0].ItemId.item()))
                        row_numbers.append(row_num)
                    xl = xl.drop(xl.index[[int(row_num) - 2 for row_num in row_numbers]])
                    xl.to_excel(self.path, index=False)
                    return self.upload(flag)

        except Exception as why:
            print('{} is failed finally cause of {}'.format(self.path, why))
            return False


    def remark_data(self, type, muban_id):
        sql = ''
        params = ()
        now = str(datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S'))
        if type == 'single' and self.path.find('SMT1') != -1:
            file_name_str = self.path[self.path.find('SMT1'):]
            list = file_name_str.split('.')
            sql = "update proCenter.oa_smtImportToIbayLog set mubanId=%s,completeDate1=%s,status1=%s where ibaySuffix=%s and sku=%s;"
            params = (muban_id, now, 1, list[2], list[1])
        if type == 'multiple' and self.path.find('SMT2') != -1:
            file_name_str = self.path[self.path.find('SMT2'):]
            list = file_name_str.split('.')
            sql = "update proCenter.oa_smtImportToIbayLog set completeDate2=%s,status2=%s where mubanId=%s;"
            params = (now, 1, list[1])

        try:
            self.warehouse_cur.execute(sql, params)
            self.warehouse_con.commit()
            print('success to remark data!')
        except Exception as why:
            print('success to remark data cause of {}'.format(why))



    def run(self, type = 'single'):
        now = str(datetime.datetime.now())
        res = self.upload(type)    # 导入单属性信息

        if res:
            # with lock:
            self.remark_data(type, res)  # 标记结果
            os.remove(self.path)    # 删除excel文件
            print('{}:successful to upload {}'.format(now, self.path))
        else:
            print('{}:failed to upload {}'.format(now, self.path))



class Export(BaseService):
    """
    Export goods info to excel file
    """
    def __init__(self):
        super().__init__()
        self.path = '../../runtime/'
        self.single = {
            "MubanId": "",
            "Selleruserid": "",
            "Category1": 200001479,
            "packageLength": "",
            "packageWidth": "",
            "packageHeight": "",
            "grossWeight": "",
            "isPackSell": 0,
            "baseUnit": "",
            "addUnit": "",
            "addWeight": 0,
            "freighttemplate": "Shipping Cost Template for New Sellers",
            "promisetemplate": 0,
            "itemtitle": "",
            "SKU": "",
            "ImageUrl": "",
            "productPrice": "",
            "Quantity": 1000,
            "lotNum": 1,
            "productunit": "件/个",
            "groupid": "",
            "wsvalidnum": 30,
            "packageType": 0,
            "bulkOrder": "",
            "bulkDiscount": "",
            "deliverytime": 7,
            "Description": "",
            "Descriptionmobile": "",
            "Remarks": "",
            "AutoDelay": 1,
            "Publicmubanedit": ""
        }




    # 获取产品单属性 数据
    def get_data(self):
        sql = 'select  * from proCenter.oa_smtImportToIbayLog where status1=0 and IFNULL(mubanId,0)=0'
        self.warehouse_cur.execute(sql)
        self.warehouse_con.commit()
        ret = self.warehouse_cur.fetchall()
        return ret



    async def deal_data(self, data):
        for item in data:
            res = self.single
            res['Selleruserid'] = item['ibaySuffix']
            res['SKU'] = item['SKU']


            smtSql = ('select  * from proCenter.oa_smtGoods ae left join proCenter.oa_goodsinfo g on g.id=ae.infoId '  +
                  ' where goodsCode = %s;')
            self.warehouse_cur.execute(smtSql, (item['SKU'], item['SKU']))
            smtQuery = self.warehouse_cur.fetchone()
            if smtQuery:
                res['ImageUrl'] = smtQuery['imageUrl']
                res['productPrice'] = smtQuery['productPrice']
                res['Quantity'] = smtQuery['quantity']
                res['Description'] = smtQuery['description']
                res["packageLength"] = smtQuery['packageLength'],
                res["packageWidth"] = smtQuery['packageWidth'],
                res["packageHeight"] = smtQuery['packageHeight'],
                res["grossWeight"] = smtQuery['grossWeight'],
                res['itemtitle'] = smtQuery['itemtitle']
                res['Category1'] = smtQuery['category1']


            now = str(datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
            # print(res)
            file_name = self.path + 'SMT1' + '.' + res['SKU'] + '.' + res['Selleruserid'] + '.' + now + '.xls'
            generate(res, file_name)  # 导出单属性数据




    async def work(self):
        l = Lock()
        p = Pool(16, initializer=init, initargs=(l,))
        paths = []
        for input_file in os.listdir(self.path):
            paths.append(self.path + input_file)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            # p.map_async(work, paths)
        # p.map_async(partial(uploader, 'single'), paths)

        p.map_async(upload_single, paths)

        p.close()
        p.join()



    async def run(self):
        try:
            # 获取单属性数据
            list = self.get_data()  # 获取数据
            if list:
                await self.deal_data(list)  # 处理数据 并导出表格

            await self.work()       #导入单属性数据，记录结果
            print('success to import goods info!')
        except Exception as why:
            print('failed to import goods info cause of {}'.format(why))



def init(l):
    global lock
    lock = l


def upload_single(path):
    uploader = Uploader(path)
    uploader.run('single')




if __name__ == '__main__':

    start = time.time()
    export = Export()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(export.run())
    end = time.time()
    print(f'it takes {end - start} seconds')





