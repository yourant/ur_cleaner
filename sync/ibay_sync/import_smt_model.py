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


    def upload(self, type):
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
                res = ''
                data = {'mubanxls': ('report.xls', files, 'application/vnd.ms-excel')}
                res = self.session.post(self.upload_url[type], files=data)
                soup = BeautifulSoup(res.content, features='html.parser')
            try:
                html = soup.find(text=re.compile("导入成功"))
                content = re.findall(r'\d+', html)
                print(soup)
                if html:
                    return True
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
                    return self.upload(type)

        except Exception as why:
            print('{} is failed finally cause of {}'.format(self.path, why))
            return False


    def remark_data(self, type):
        list = self.path.split('.')


    def run(self, type = 'single'):
        now = str(datetime.datetime.now())
        # if type == 'single' and self.path.index('SMT1') :
            # res = self.upload(type)
        # else:
        #     res = False
        self.remark_data(type)
        print(self.path)
        # if res:
            # with lock:
            #     os.remove(self.path)
            #     print('{}:successful to upload {}'.format(now, self.path))
        # else:
        #     print('{}:failed to upload {}'.format(now, self.path))



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
        self.multiple = {

        }

    # 获取产品 数据
    def get_data(self):
        sql = 'select  * from proCenter.oa_smtImportToIbayLog where status1=0 or status2=0'
        self.warehouse_cur.execute(sql)
        self.warehouse_con.commit()
        ret = self.warehouse_cur.fetchall()
        return ret


    async def deal_data(self, data):
        for item in data:
            res = self.single
            res['Selleruserid'] = item['ibaySuffix']
            res['SKU'] = item['SKU']
            res["packageLength"] = 10,
            res["packageWidth"] = 10,
            res["packageHeight"] = 3,
            res["grossWeight"] = 0.07,
            res['itemtitle'] = 'Smt Test Title'
            wishSql = ('select  * from proCenter.oa_wishGoods ae left join proCenter.oa_goodsinfo g on g.id=ae.infoId '  +
                  ' where goodsCode = %s or sku = %s;')
            self.warehouse_cur.execute(wishSql, (item['SKU'], item['SKU']))
            wishQuery = self.warehouse_cur.fetchone()
            if wishQuery:
                res['ImageUrl'] = wishQuery['mainImage']
                res['productPrice'] = wishQuery['price']
                res['Quantity'] = wishQuery['inventory']
                res['Description'] = wishQuery['description']
            else:
                ebaySql = (
                        'select  * from proCenter.oa_ebayGoods ae left join proCenter.oa_goodsinfo g on g.id=ae.infoId ' +
                        ' where goodsCode = %s or sku = %s;')
                self.warehouse_cur.execute(ebaySql, (item['SKU'], item['SKU']))
                ebayQuery = self.warehouse_cur.fetchone()
                if ebayQuery:
                    res['ImageUrl'] = ebayQuery['mainImage']
                    res['productPrice'] = ebayQuery['nowPrice']
                    res['Quantity'] = ebayQuery['quantity']
                    res['Description'] = ebayQuery['description']

            generate(res, self.path, 'SMT1')  # 导出单属性数据




    async def work(self, type = 'single'):
        l = Lock()
        p = Pool(16, initializer=init, initargs=(l,))
        paths = []
        for input_file in os.listdir(self.path):
            paths.append(self.path + input_file)
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            # p.map_async(work, paths)
        # p.map_async(partial(uploader, 'single'), paths)
        if type == 'single':
            p.map_async(upload_single, paths)
        else:
            p.map_async(upload_multiple, paths)
        p.close()
        p.join()



    async def run(self):
        # list = self.get_data()  # 获取数据
        # if list:
        #     await self.deal_data(list)  # 处理数据 并导出表格


        await self.work('single')
        # await self.do_upload('single')





def init(l):
    global lock
    lock = l


def upload_single(path):
    uploader = Uploader(path)
    uploader.run('single')

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





