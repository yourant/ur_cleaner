#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-03-14 10:07
# Author: turpure

import time
import requests
import json
from multiprocessing.pool import ThreadPool as Pool
import re
from bs4 import BeautifulSoup
import sys
from src.services.base_service import BaseService
from pyppeteer import launch
from configs.config import Config


"""
vova 任務采集。
"""


class Worker(BaseService):

    def __init__(self):
        # config = Config().config
        # self.token = config['ur_center']['token']
        super().__init__()



    def get_tasks(self):
        """
        获取采集状态为【待采集】，且2天之内，10分钟之前的任务
        :return:
        """
        # sql = ("select proId from proCenter.oa_dataMine where platform='joom' and progress in ('待采集', '采集失败')  "
        sql = ("select * from proCenter.oa_dataMine where platform='vova' and progress in ('待采集') and detailStatus='未完善'"
               " and timestampdiff(day,createTime,now())<=30 and timestampdiff(MINUTE,createTime,now()) >=2 ORDER BY id DESC LIMIT 1")
        self.warehouse_cur.execute(sql)
        ret = self.warehouse_cur.fetchall()
        for row in ret:
            yield row


    def get_style_info(self, soup, type=''):
        dic = {}
        try:
            if type == 'img':
                html = soup.select('.prod-thumb-big > div > div')
                for row in html:
                    index = row.get('data-img-id')
                    value = row.find('img').get('data-src').replace('500_500','150_150')
                    dic['name'] = row.find('img').get('alt')
                    dic[index] = 'http:' + value
            else:
                html = soup.select('.prod-styles > div > div > div > span')
                for row in html:
                    index = row.get('data-id')
                    value = row.getText()
                    dic[index] = value
        except Exception as why:
            self.logger.error(f'fail to get {type} info cause of {why}')

        return dic

    def redo_tasks(self, row):
        # try:
        task_id = row['proId']
        #获取html信息
        response = requests.get(task_id)
        soup = BeautifulSoup(response.content, features='html.parser')
        title = soup.head.title.getText()
        # imageHtml = soup.find(class_='prod-thumb-big').find_all('div')
        image = self.get_style_info(soup, 'img')
        styles = self.get_style_info(soup)
        print(image)
        print(styles)


        # 获取 SKU 信息
        product_id = task_id.split('-')[-1]
        url = "https://www.vova.com/ajax.php?act=get_goods_sku_style&virtual_goods_id=" + product_id[1:]
        skuRes = requests.get(url)
        res = skuRes.json()
        if res['code'] == 0:
            imageIdDic = res['data']['image_list']
            i = 1
            for sku in res['data']['skuList']:
                item = {}
                item['mid'] = row['id']
                item['parentId'] = row['goodsCode']
                if i > 9:
                    item['childId'] = row['goodsCode'] + '_' + str(i)
                else:
                    item['childId'] = row['goodsCode'] + '_0' + str(i)
                item['quantity'] = sku['storage'],
                item['price'] = sku['display_shop_price_exchange']
                item['msrPrice'] = sku['display_market_price_exchange']
                item['shippingTime'] = '15-50',
                item['shipping'] = 0
                item['shippingWeight'] = 0
                item['proName'] = image['name']
                skuImgId = str(imageIdDic[str(sku['sku_id'])])
                print(type(skuImgId))
                item['varMainImage'] = image[skuImgId]
                print(item['varMainImage'])

                item['description'] = ''
                item['color'] = ''
                item['proSize'] = ''
                # item['extra_image0'], item['extra_image1'], item['extra_image2'],
                # item['extra_image3'], item['extra_image4'], item['extra_image5'],
                # item['extra_image6'], item['extra_image7'], item['extra_image8'],
                # item['extra_image9'], item['extra_image10'], item['mainImage']
                print(sku)



        # except Exception as why:
        #     self.logger.error(f'fail to fetch cause of {type(why)}')





    # async def intercept_response(self, res, job_id):
    #     resource_type = res.request.resourceType
    #     if resource_type in ['xhr']:
    #         url = res.url
    #         if re.match(r'.*/products/[a-z0-9\-]+\?currency', url):
    #             try:
    #                 ret = await res.json()
    #                 if ret:
    #                     rows = self.parse(ret, self.color_dict)
    #                     # 异步插入
    #                     task = asyncio.ensure_future(self.save_data(rows, job_id=job_id))
    #                     await asyncio.gather(task)
    #             except Exception as why:
    #                 self.logger.info(f'fail to fetch response cause of {why}')


    def insert(self, rows, job_id):
        insert_sql = ("insert oa_dataMineDetail"
                          "(mid,parentId,proName,description,"
                          "tags,childId,color,proSize,quantity,"
                          "price,msrPrice,shipping,shippingWeight,"
                          "shippingTime,varMainImage,extraImage0,"
                          "extraImage1,extraImage2,extraImage3,"
                          "extraImage4,extraImage5,extraImage6,"
                          "extraImage7,extraImage8,extraImage9,"
                          "extraImage10,mainImage"
                          ") "
                          "values( %s,%s,%s,%s,%s,%s,%s,%s,"
                          "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                          "%s,%s,%s,%s,%s,%s,%s,%s)")
        update_sql = "update oa_dataMine set progress=%s where id=%s"
        code_sql = "select goodsCode from oa_dataMine where id=%s"
        main_image_sql = "update oa_dataMine set mainImage=%s where id=%s"
        is_done_sql = 'select progress from oa_dataMine where id= %s'
        try:

            self.warehouse_cur.execute(is_done_sql, (job_id,))
            is_done_ret = self.warehouse_cur.fetchone()
            if is_done_ret[0] == '采集成功':
                return
            self.warehouse_cur.execute(code_sql, (job_id,))
            code_ret = self.warehouse_cur.fetchone()
            code = code_ret[0]
            index = 1
            for row in rows:
                row['mid'] = job_id
                row['parentId'] = code
                row['childId'] = code + '_' + '0' * (2 - len(str(index))) + str(index)
                index += 1
                self.warehouse_cur.execute(main_image_sql, (row['mainImage'], job_id))
                self.warehouse_cur.execute(insert_sql,
                            (row['mid'], row['parentId'], row['proName'], row['description'],
                             row['tags'], row['childId'], row['color'], row['proSize'], row['quantity'],
                             float(row['price']), float(row['msrPrice']), row['shipping'],
                             float(row['shippingWeight']),
                             row['shippingTime'],
                             row['varMainImage'], row['extra_image0'], row['extra_image1'], row['extra_image2'],
                             row['extra_image3'], row['extra_image4'], row['extra_image5'],
                             row['extra_image6'], row['extra_image7'], row['extra_image8'],
                             row['extra_image9'], row['extra_image10'], row['mainImage']))

            self.warehouse_cur.execute(update_sql, (u'采集成功', job_id))
            self.warehouse_con.commit()
            self.logger.info('fetching %s' % job_id)
            print(f'success to fetch {job_id}')
        except Exception as why:
            self.logger.error('%s while fetching %s' % (why, job_id))
            print(f'failed to fetch {job_id}')
            self.warehouse_cur.execute(update_sql, (u'采集失败', job_id))
            self.warehouse_con.commit()


    def run(self):
        BeginTime = time.time()
        # try:
        tasks = self.get_tasks()
        pl = Pool(50)
        pl.map(self.redo_tasks, tasks)
        pl.close()
        pl.join()

        # except Exception as why:
        #     self.logger.error(why)
        # finally:
        #     self.close()
        print('程序耗时{:.2f}'.format(time.time() - BeginTime))  # 计算程序总耗时


if __name__ == '__main__':
    worker = Worker()
    worker.run()


