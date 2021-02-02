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
        # sql = "SELECT id,suffix,sku FROM proCenter.oa_shopifyImportToBackstageLog where IFNULL(product_id,'')='' limit 1"
        sql = ("SELECT * FROM proCenter.oa_shopifyImportToBackstageLog where IFNULL(product_id,'')<>'' " +
               " and (IFNULL(imgStatus, '') <> 'success' OR IFNULL(collectionStatus, '') <> 'success') "
               )
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
            sql = "update proCenter.oa_shopifyImportToBackstageLog set imgStatus=%s,imgContent=%s," \
                  "collectionStatus=%s,collectionContent=%s,updateDate=%s where id=%s"
            update_time = str(datetime.datetime.now().strftime('%Y-%m-%d %H-%M-%S'))
            log_params = (params['imgStatus'], params['imgContent'], params['collectionStatus'],
                          params['collectionContent'], update_time, params['id'])

            self.warehouse_cur.execute(sql, log_params)
            self.warehouse_con.commit()
        except Exception as why:
            self.logger.error('fail to save log info cause of {} '.format(why))

    def get_oa_product_sku_info(self, sku):
        sql = 'select  gs.* from proCenter.oa_shopifyGoodsSku gs' \
              ' left join proCenter.oa_shopifyGoods g on g.infoId = gs.infoId ' \
              ' where g.sku = %s'
        self.warehouse_cur.execute(sql, sku)
        rows = self.warehouse_cur.fetchall()
        for row in rows:
            yield row

    def get_collection(self, sku, suffix):
        sql = 'select  gc.* from proCenter.oa_shopifyGoodsCollection gc' \
              ' left join proCenter.oa_shopifyGoods g on g.infoId = gc.infoId ' \
              ' where g.sku = %s and gc.suffix = %s'
        self.warehouse_cur.execute(sql, (sku, suffix))
        rows = self.warehouse_cur.fetchall()
        for row in rows:
            yield row

    def upload_img_and_collection(self, row):
        task_id = row['id']
        sku = row['sku']
        suffix = row['suffix']
        product_id = row['product_id']

        token = self.get_shopify_password(suffix)
        api_key = token['api_key']
        password = token['password']
        url_head = 'https://' + api_key + ':' + password + '@'
        url_body = suffix + '.myshopify.com/admin/api/2019-07/'
        url = url_head + url_body + 'products/' + product_id + '.json'
        params = {'imgStatus': '', 'imgContent': "", 'collectionStatus': '', 'collectionContent': "", 'id': task_id}
        try:
            # 上传图片
            if row['imgStatus'] != 'success':
                err = list()
                product_sku = list(self.get_oa_product_sku_info(sku))
                # product = self.get_oa_product_info(sku)
                res = requests.get(url)
                product = res.json()
                for img in product['product']['images']:
                    new_sku_arr = list()
                    variant_ids = list()
                    try:
                        img_suffix = img['src'].split('_')[1].split('.jpg')[0]
                    except:
                        img_suffix = ''
                    # 获取图片对应的产品SKU
                    for s in product_sku:
                        sku_img_suffix = s['linkUrl'].split('_')[1]
                        if img_suffix == sku_img_suffix:
                            new_sku_arr.append(s['sku'])
                    # 获取图片对应的SKU ID
                    for sk in new_sku_arr:
                        for var in product['product']['variants']:
                            if sk == var['sku']:
                                variant_ids.append(var['id'])
                    # 更新图片对应的SKU ID
                    if len(variant_ids) > 0:
                        img_url = url_head + url_body + 'products/' + str(product_id) + '/images/' + str(
                            img['id']) + '.json'
                        img_data = {'image': {'id': img['id'], 'variant_ids': variant_ids}}
                        img_res = requests.put(img_url, data=json.dumps(img_data),
                                               headers={'Content-Type': 'application/json'})
                        img_ret = img_res.json()
                        if 'image' not in img_ret:
                            err.append({'img_id': img['id'], 'variant_ids': variant_ids, 'msg': 'relation failed'})
                if err:
                    params['imgContent'] = json.dumps(err)
                    params['imgStatus'] = ''
                else:
                    params['imgStatus'] = 'success'
                    params['imgContent'] = ''
                self.update_log(params)
                # self.logger.error('success to upload cause of {}'.format(params['content']))

            # 添加 collection
            if row['collectionStatus'] != 'success':
                params['imgStatus'] = row['imgStatus']
                collection = self.get_collection(sku, suffix)
                coll_err = list()
                for item in collection:
                    coll_data = {'custom_collection': {'id': item['coll_id'], 'collects': [{"product_id": product_id}]}}
                    coll_url = url_head + url_body + 'custom_collections/' + item['coll_id'] + '.json'
                    coll_res = requests.put(coll_url, data=json.dumps(coll_data),
                                            headers={'Content-Type': 'application/json'})
                    coll_ret = coll_res.json()
                    if 'custom_collection' not in coll_ret and 'already exists in this collection' not in json.dumps(coll_ret):
                        coll_err.append(
                            {'coll_id': item['coll_id'], 'product_id': product_id, 'msg': 'relation failed'})

                if coll_err:
                    params['collectionContent'] = json.dumps(coll_err)
                    params['collectionStatus'] = ''
                else:
                    params['collectionStatus'] = 'success'
                    params['collectionContent'] = ''
                self.update_log(params)
        except Exception as e:
            self.logger.error(e)

    async def work(self):
        try:
            tasks = self.get_shopify_tasks()
            for task in tasks:
                self.upload_img_and_collection(task)
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
