#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-05-22 14:49
# Author: henry

import time
import asyncio
import datetime
from sync.ibay_sync.smt_ibay_server import generate
from src.services.base_service import BaseService




class Download(BaseService):
    """
    Export goods info to excel file
    """
    def __init__(self):
        super().__init__()
        self.path = '../../runtime/smt/'




    # 获取产品单属性 数据
    def get_data(self):
        sql = 'select  * from proCenter.oa_smtImportToIbayLog where status1=0 and IFNULL(mubanId,0)=0'
        self.warehouse_cur.execute(sql)
        self.warehouse_con.commit()
        ret = self.warehouse_cur.fetchall()
        return ret



    def deal_data(self, data):
        for item in data:
            res = {
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
            res['Selleruserid'] = item['ibaySuffix']
            res['SKU'] = item['SKU']


            smtSql = ('select  * from proCenter.oa_smtGoods ae left join proCenter.oa_goodsinfo g on g.id=ae.infoId '  +
                  ' where goodsCode = %s;')
            self.warehouse_cur.execute(smtSql, (item['SKU']))
            smtQuery = self.warehouse_cur.fetchone()
            if smtQuery:
                res['ImageUrl'] = smtQuery['imageUrl']
                res['productPrice'] = smtQuery['productPrice']
                res['Quantity'] = smtQuery['quantity']
                res['Description'] = smtQuery['description'].replace("\n", "</br>")
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



    async def run(self):
        try:
            # 获取单属性数据
            list = self.get_data()  # 获取数据
            if list:
                self.deal_data(list)  # 处理数据 并导出表格
                self.logger.error('Success to download goods templates')
            # else:
            #     self.logger.info('No goods template need to download')
        except Exception as why:
            self.logger.error('Failed to download goods templates cause of {}'.format(why))
        finally:
            self.close()



if __name__ == '__main__':

    start = time.time()
    export = Download()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(export.run())
    end = time.time()
    date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
    print(date + f' it takes {end - start} seconds')





