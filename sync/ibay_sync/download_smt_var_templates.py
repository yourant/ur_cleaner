#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-05-22 14:49
# Author: henry

import time
import pickle
import json
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

    # 获取产品多属性 数据
    def get_var_data(self):
        sql = 'select  * from proCenter.oa_smtImportToIbayLog where status1=1 and status2=0'
        self.warehouse_cur.execute(sql)
        self.warehouse_con.commit()
        ret = self.warehouse_cur.fetchall()
        return ret

    def deal_var_data(self, data):
        for item in data:
            rows = []
            smtSql = (
                'select  * from proCenter.oa_smtGoodsSku ae inner join proCenter.oa_goodsinfo g on g.id=ae.infoId '
                ' inner join proCenter.oa_smtGoods gg on gg.infoId=g.id  where goodsCode = %s;')
            self.warehouse_cur.execute(smtSql, item['SKU'])
            smtQuery = self.warehouse_cur.fetchall()
            if smtQuery:
                for row in smtQuery:
                    res = {
                        "mubanid": "",
                        "sku": "",
                        "quantity": "",
                        "price": "",
                        "pic_url": "",
                        "skuimage": 'Color',
                        "varition1": "Color:Black(黑色)",
                        "name1": "",
                        "varition2": "",
                        "name2": "",
                        "varition3": "",
                        "name3": "",
                        "varition4": "",
                        "name4": "",
                        "varition5": "",
                        "name5": ""
                    }
                    res['mubanid'] = item['mubanId']
                    res['sku'] = row['sku']
                    res['quantity'] = row['quantity']
                    res['price'] = row['price']
                    res['pic_url'] = row['pic_url']
                    var = self.trim_var_data(row)
                    res['skuimage'] = var['skuimage']
                    res['varition1'] = var['varition1']
                    res['varition2'] = var['varition2']

                    # if row['category1'] in {100007323}:  # 戒指 TODO
                    #     res["skuimage"] = 'Main Stone Color'
                    #     res["varition1"] = "Main Stone Color:Black(黑色)"
                    #     res["varition2"] = "Ring Size:" + row['size'] + '(' + row['size'] + ')'
                    # if row['category1'] in {100007322, 200000171, 100007324, 200000168, 200000147, 200000162}:  #
                    #     res["skuimage"] = 'Metal Color'
                    #     res["varition1"] = "Metal Color:Red(红色)"

                    rows.append(res)
            # print(rows)
            now = str(datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
            file_name = self.path + 'SMT2' + '.' + str(rows[0]['mubanid']) + '.' + now + '.xls'
            generate(rows, file_name)  # 导出单属性数据

    def trim_var_data(self, data):
        sql = "select name,value from aliexpress_specifics where categoryid=%s and isskuattribute=1 order by customizedpic desc"
        self.ibay_cur.execute(sql, (data['category1'],))
        rows = self.ibay_cur.fetchall()
        ret = dict()
        ret['skuimage'] = rows[0][0]
        ret['varition1'] = rows[0][0] + ':' + data['color']
        ret['varition2'] = ''
        for row in rows:
            # print(row)
            if ('size' in row[0] or 'Size' in row[0]) and data['size']:
                ret['varition2'] = row[0] + ':' + data['size']
        return ret

    async def run(self):
        try:
            # 获取单属性数据
            data = self.get_var_data()  # 获取数据
            if data:
                self.deal_var_data(data)  # 处理数据 并导出表格
                self.logger.error('Success to download goods var templates')
            else:
                self.logger.info('No goods var template need to download')
        except Exception as why:
            self.logger.error('Failed to download goods var templates cause of {}'.format(why))
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
