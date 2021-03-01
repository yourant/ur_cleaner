#! usr/bin/env/python3
# coding:utf-8
# @Time: 2021-01-20 16:30
# Author: henry


import os
import asyncio
import time
import datetime
from src.services.base_service import CommonService
import requests
import json


class Worker(CommonService):
    """
    worker template
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.sql_name = 'mysql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)

        self.warehouse_cur = self.base_dao.get_cur(self.sql_name)
        self.warehouse_con = self.base_dao.get_connection(self.sql_name)

    def close(self):
        self.base_dao.close_cur(self.cur)
        self.base_dao.close_cur(self.warehouse_cur)

    def get_shopify_password(self, suffix):
        # sql = "SELECT apikey as api_key,hostname,password FROM [dbo].[S_ShopifySyncInfo] where hostname='speedacx'"
        sql = "SELECT apikey as api_key,password FROM [dbo].[S_ShopifySyncInfo] where hostname=%s"
        self.cur.execute(sql, suffix)
        ret = self.cur.fetchone()
        return ret

    def get_shopify_tasks(self):
        sql = "SELECT l.id,l.suffix,sku,flag  FROM proCenter.oa_shopifyImportToBackstageLog as l " \
              "LEFT JOIN proCenter.oa_shopify s ON s.account=l.suffix " \
              "where IFNULL(product_id,'')='' "
        # sql = ("SELECT * FROM proCenter.oa_shopifyImportToBackstageLog where IFNULL(product_id, '') = '' " +
        #        " OR IFNULL(imgStatus, '') <> 'success' OR IFNULL(collectionStatus, '') = 'success' " )
        self.warehouse_cur.execute(sql)
        ret = self.warehouse_cur.fetchall()
        for row in ret:
            yield row

    def get_oa_product_info(self, sku):
        sql = 'select  * from proCenter.oa_shopifyGoods where sku = %s'
        self.warehouse_cur.execute(sql, sku)
        ret = self.warehouse_cur.fetchone()
        return ret

    def update_log(self, params):
        try:
            sql = "update proCenter.oa_shopifyImportToBackstageLog set product_id=%s,productStatus=%s, " \
                  " productContent=%s,updateDate=%s where id=%s"
            update_time = str(datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S'))
            log_params = (
            params['product_id'], params['productStatus'], params['productContent'], update_time, params['id'])

            self.warehouse_cur.execute(sql, log_params)
            self.warehouse_con.commit()
        except Exception as why:
            self.logger.error('fail to save log info cause of {} '.format(why))

    def get_oa_product_sku_info(self, product):
        sql = 'select  * from proCenter.oa_shopifyGoodsSku where infoId = %s'
        self.warehouse_cur.execute(sql, product['infoId'])
        rows = self.warehouse_cur.fetchall()
        for row in rows:
            yield row

    @staticmethod
    def _parse_vars(product, rows, flag):
        out = {'variation': list(), 'options': list(), 'images': list(), 'tags': ''}

        # sql = 'select  * from proCenter.oa_shopifyGoodsSku where infoId = %s'
        # self.warehouse_cur.execute(sql, product['infoId'])
        # rows = self.warehouse_cur.fetchall()
        value1 = list()
        value2 = list()
        # 处理附加图
        images = list()
        extra_img = product['extraImages'].split("\n")
        for img in extra_img:
            images.append({'src': img})
        for row in rows:
            var = dict()
            var['sku'] = row['sku']
            var['inventory_quantity'] = int(row['inventory'])
            var['inventory_policy'] = 'continue'
            var['fulfillment_service'] = 'manual'
            var['inventory_management'] = 'shopify'
            var['requires_shipping'] = True
            var['taxable'] = True
            if row['color']:
                var['option1'] = row['color']
                if row['color'] not in value1:
                    value1.append(row['color'])
            if row['size']:
                option_type = 'option2' if row['color'] else 'option1'
                var[option_type] = row['size']
                if row['size'] not in value2:
                    value2.append(row['size'])

            var['price'] = int(row['price'])
            # row['compare_at_price'] = row['msrp']
            var['weight'] = row['weight']
            var['weight_unit'] = 'g'
            out['variation'].append(var)
            sku_img = {'src': row['linkUrl']}
            if sku_img not in images:
                images.append(sku_img)
        # 处理 多属性
        if value1 and value2:
            out['options'] = [{'name': 'Color', 'values': value1}, {'name': 'Size', 'values': value2}]
        # 处理图片
        out['images'] = [{'src': product['mainImage']}] + images
        # 处理 标签
        if flag:
            out['tags'] = ('Color_' if value1 else '') + ',Color_'.join(value1) + \
                      (',Size_' if value2 else '') + ',Size_'.join(value2)
        else:
            out['tags'] = ','.join(value1) + ','.join(value2)

        if product['style']:
            if flag:
                style = ('Style_' if product['style'] else '') + product['style'].replace(',', ',Style_')
                out['tags'] = out['tags'] + (',' if out['tags'] else '') + style
            else:
                out['tags'] = out['tags'] + (',' if out['tags'] else '') + product['style']
        if product['length']:
            if flag:
                length = ('Length_' if product['length'] else '') + product['length'].replace(',', ',Length_')
                out['tags'] = out['tags'] + (',' if out['tags'] else '') + length
            else:
                out['tags'] = out['tags'] + (',' if out['tags'] else '') + product['length']
        if product['sleeveLength']:
            if flag:
                sleeve = ('SleeveLength_' if product['sleeveLength'] else '') + \
                         product['sleeveLength'].replace(',', ',SleeveLength_')
                out['tags'] = out['tags'] + (',' if out['tags'] else '') + sleeve
            else:
                out['tags'] = out['tags'] + (',' if out['tags'] else '') + product['sleeveLength']
        if product['neckline']:
            if flag:
                neckline = ('Neckline_' if product['neckline'] else '') + product['neckline'].replace(',', ',Neckline_')
                out['tags'] = out['tags'] + (',' if out['tags'] else '') + neckline
            else:
                out['tags'] = out['tags'] + (',' if out['tags'] else '') + product['neckline']
        if product['other']:
            out['tags'] = out['tags'] + (',' if out['tags'] else '') + product['other']
        # print(out['tags'])
        return out

    def upload_products(self, row):
        task_id = row['id']
        sku = row['sku']
        suffix = row['suffix']
        flag = row['flag']

        token = self.get_shopify_password(suffix)
        api_key = token['api_key']
        password = token['password']
        endpoint = 'products.json'
        url_head = 'https://' + api_key + ':' + password + '@'
        url_body = suffix + '.myshopify.com/admin/api/2019-07/'
        url = url_head + url_body + endpoint
        # if True:
        try:
            product = self.get_oa_product_info(sku)
            params = {'product_id': '', 'productStatus': '', 'productContent': "", 'id': task_id}
            if not product:
                params['productContent'] = f"The product {sku} is not exist!",
                self.update_log(params)
                self.logger.error('failed to upload cause of product {} is not exist'.format(sku))
            elif not product['title']:
                params['productContent'] = f"The product {sku} title is empty!",
                self.update_log(params)
                self.logger.error('failed to upload cause of product {} title is empty'.format(sku))
            else:
                item = dict()
                # item['sku'] = product['sku']
                item['title'] = product['title']
                item['body_html'] = product['description'].replace("\n", '<br>')
                # item['vendor'] = ''
                item['product_type'] = product['productType']
                product_sku = list(self.get_oa_product_sku_info(product))
                sku_info = self._parse_vars(product, product_sku, flag)
                item['variants'] = sku_info['variation']
                item['images'] = sku_info['images']
                item['tags'] = sku_info['tags'].split(',')
                # print(sku_info)
                if sku_info['options']:
                    item['options'] = sku_info['options']

                # print(item)
                data = json.dumps({'product': item}, ensure_ascii=False)
                # print(data)
                try:
                    response = requests.post(url, data=data, headers={'Content-Type': 'application/json'})
                    ret = response.json()
                    params['product_id'] = str(ret['product']['id'])
                    params['productStatus'] = 'success'
                    params['productContent'] = ''
                except Exception as why:
                    params['productContent'] = "Failed cause of an unknown mistake, it is probably a coding problem."
                    self.logger.error(why)
                self.update_log(params)

        except Exception as e:
            self.logger.error(e)

    async def work(self):
        try:
            tasks = self.get_shopify_tasks()
            for task in tasks:
                self.upload_products(task)
        except Exception as why:
            self.logger.error('fail to count sku cause of {} '.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()


if __name__ == "__main__":
    start = time.time()
    worker = Worker()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(worker.work())
    end = time.time()
    date = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(end))
    print(date + f' it takes {end - start} seconds')
