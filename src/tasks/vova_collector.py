#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-03-14 10:07
# Author: turpure

import time
import datetime
import requests
from multiprocessing.pool import ThreadPool as Pool
import re
from bs4 import BeautifulSoup
from src.services.base_service import BaseService


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
        sql = ("select * from proCenter.oa_dataMine where platform='vova' and progress in ('待采集', '采集失败') and detailStatus='未完善'"
               " and timestampdiff(day,createTime,now())<=30 and timestampdiff(MINUTE,createTime,now()) >=2 ")
        #  and id=121645

        self.warehouse_cur.execute(sql)
        ret = self.warehouse_cur.fetchall()
        for row in ret:
            yield row

    # 获取 图片字典 颜色字典  大小字典
    def get_style_info(self, soup, type=''):
        dic = {}
        try:
            if type == 'img':
                dic['img'] = {}
                html = soup.select('.prod-thumb-big > div > div')
                for row in html:
                    index = row.get('data-img-id')
                    value = row.find('img').get('data-src').replace('500_500','150_150')
                    dic['name'] = row.find('img').get('alt')
                    dic['img'][index]  = 'http:' + value
            else:
                html = soup.select('.prod-styles > div')
                for row in html:
                    if row.find(text=re.compile("Color")) and type == 'color':
                        items = row.select('div > div > span')
                        for item in items:
                            index = item.get('data-id')
                            value = item.getText()
                            dic[index] = value
                    if row.find(text=re.compile("Size")) and type == 'size':
                        items = row.select('div > div > span')
                        for item in items:
                            index = item.get('data-id')
                            value = item.getText()
                            dic[index] = value
        except Exception as why:
            self.logger.error(f'fail to get {type} info cause of {why}')
        return dic

    # 处理 extra_images
    def get_extra_images(self, images):
        extra_images = {}
        i = 0
        for img in images:
            index = 'extra_image' + str(i)
            extra_images[index] = images[img]
            i = i + 1
            if i == 11:
                break
        if len(images) < 11:
            for j in range(10 - len(images) + 1):
                inx = 'extra_image' + str(j + len(images))
                extra_images[inx] = ''
        return extra_images



    def redo_tasks(self, row):
        try:
            task_id = row['proId']
            #获取html信息
            response = requests.get(task_id)
            soup = BeautifulSoup(response.content, features='html.parser')
            cateList = soup.select('.breadcrumb > ul > li > a')
            cateName = cateList[-1].getText()
            self.insert_vova_cate(cateName, task_id)
            image = self.get_style_info(soup, 'img')
            colors = self.get_style_info(soup, 'color')
            sizes = self.get_style_info(soup, 'size')
            #处理 sku 的 extra_images
            extra_images = self.get_extra_images(image['img'])
            try:
                description = soup.select('.prod-info > dl > dd')[0].getText()
            except:
                description = ''
            # 获取 SKU 信息
            product_id = task_id.split('-')[-1]
            url = "https://www.vova.com/ajax.php?act=get_goods_sku_style&virtual_goods_id=" + product_id[1:]
            skuRes = requests.get(url)
            res = skuRes.json()
            if res['code'] == 0:
                imageIdDic = res['data']['image_list']
                skuRows = []
                i = 1
                for sku in res['data']['skuList']:
                    item = {}
                    item['mid'] = row['id']
                    item['parentId'] = row['goodsCode']
                    item['childId'] = row['goodsCode'] + '_' + '0' * (2 - len(str(i))) + str(i)
                    item['quantity'] = sku['storage'],
                    item['price'] = sku['display_shop_price_exchange']
                    item['msrPrice'] = sku['display_market_price_exchange']
                    item['shippingTime'] = '15-50',
                    item['shipping'] = 0
                    item['shippingWeight'] = 0
                    item['proName'] = image['name']

                    skuStyles = sku['style_value_ids'].split(';')
                    item['color'] = item['proSize'] = item['tags'] = ''
                    item['description'] = description


                    for color in colors:
                        for var in skuStyles:
                            if var and color == var:
                                item['color'] = colors[str(color)]
                                break
                    for size in sizes:
                        for var in skuStyles:
                            if var and size == var:
                                item['proSize'] = sizes[str(size)]
                                break
                    skuImgId = str(imageIdDic[str(sku['sku_id'])])
                    item['mainImage'] = extra_images['extra_image0']
                    try:
                        item['varMainImage'] = image['img'][skuImgId]
                    except:
                        item['varMainImage'] = list(image['img'].values())[0]
                    skuRows.append({**item, **extra_images})
                    i = i + 1
                self.insert(skuRows, row['id'])
                # print(skuRows)
        except Exception as why:
            self.logger.error(f'fail to get sku info cause of {why}')

    def insert_vova_cate(self, cateName, task_id):
        sql = "SELECT cateId FROM proCenter.vova_category WHERE cateName=%s"
        self.warehouse_cur.execute(sql, (cateName,))
        cate = self.warehouse_cur.fetchone()
        try:
            cateId = int(cate['cateId'])
        except:
            cateId = 0
        updateTime = str(datetime.datetime.today())[:19]
        insert_sql = ("insert into proCenter.vova_dataMineCate(proId,cateId,updateTime) values(%s,%s,%s) "
                     " ON DUPLICATE KEY UPDATE cateId = values(cateId), updateTime = values(updateTime)")
        self.warehouse_cur.execute(insert_sql, (task_id,cateId,updateTime))
        self.warehouse_con.commit()


    def insert(self, rows, job_id):
        insert_sql = ("insert proCenter.oa_dataMineDetail"
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
        update_sql = "update proCenter.oa_dataMine set progress=%s where id=%s"
        main_image_sql = "update proCenter.oa_dataMine set mainImage=%s where id=%s"
        is_done_sql = 'select progress from proCenter.oa_dataMine where id= %s'

        try:
            self.warehouse_cur.execute(is_done_sql, (job_id,))
            is_done_ret = self.warehouse_cur.fetchone()
            if is_done_ret['progress'] == '采集成功':
                return
            for row in rows:
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
            # print(f'success to fetch {job_id}')
        except Exception as why:
            self.logger.error('%s while fetching %s' % (why, job_id))
            print(f'failed to fetch {job_id}')
            self.warehouse_cur.execute(update_sql, (u'采集失败', job_id))
            self.warehouse_con.commit()


    def run(self):
        BeginTime = time.time()
        try:
            tasks = self.get_tasks()
            pl = Pool(16)
            pl.map(self.redo_tasks, tasks)
            pl.close()
            pl.join()

        except Exception as why:
            self.logger.error(why)
        finally:
            self.close()
        print('程序耗时{:.2f}'.format(time.time() - BeginTime))  # 计算程序总耗时


if __name__ == '__main__':
    worker = Worker()
    worker.run()


