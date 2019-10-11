#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-10-10 16:12
# Author: turpure


from src.services.base_service import BaseService

import requests
import json


class Worker(BaseService):

    def get_products(self):
        base_url = 'http://111.231.88.85:38080/hysj_v2/ebay_api/item_infos?u_name=youran&time=1570688732&sign=2d0a3f02e005e56f65f65810efb01bff&station=america'
        res = requests.post(base_url)
        ret = res.json()['result']
        for ele in ret:
            yield (ele['item_id'], ele['main_image'], ele['title'], ele['cids'], ele['price'], ele['sold'],
                   ele['sold_the_previous_day'], ele['payment_the_previous_day'], ele['sold_the_previous_growth'],
                   ele['sales_week1'], ele['sales_week2'], ele['sales_week_growth'], ele['payment_week1'], ele['payment_week2'],
                   ele['item_location'], ele['watchers'], ele['last_modi_time'], ele['stat_time'],
                   ele['gen_time'], ele['seller'], ele['store'], ele['store_location'], ele['category_structure'],
                   ele['sales_three_day1'], ele['sales_three_day2'], ele['sales_three_day_growth'], ele['payment_three_day1'],
                   ele['payment_three_day2'], ele['visit'], ele['sales_three_day_flag'], ele['item_url'], ele['marketplace'],
                   ele['popular'], 'US')
        return ret

    def save_products(self, rows):
        sql = ('insert into proEngine.ebay_products (item_id,main_image,title,cids,'
               'price,sold,sold_the_previous_day,payment_the_previous_day,sold_the_previous_growth,'
               'sales_week1,sales_week2,sales_week_growth,payment_week1,payment_week2,item_location,'
               'watchers,last_modi_time,stat_time,gen_time,seller,store,store_location,category_structure,'
               'sales_three_day1,sales_three_day2,sales_three_day_growth,payment_three_day1,payment_three_day2,'
               'visit,sales_three_day_flag,item_url,marketplace,popular, station) values ('
               '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)')
        self.warehouse_cur.executemany(sql, rows)
        self.warehouse_con.commit()

    def run(self):
        try:
            rows = self.get_products()
            self.save_products(rows)
            self.logger.info('success to get ebay products from haiYing')
        except Exception as why:
            self.logger.error(f'fail to get ebay products from haiYing cause of {why}')
        finally:
            self.close()


if __name__ == '__main__':
    worker = Worker()
    worker.run()
