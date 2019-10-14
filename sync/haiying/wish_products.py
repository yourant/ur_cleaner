#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-10-10 16:12
# Author: turpure


from src.services.base_service import BaseService

import requests
import json


class Worker(BaseService):

    def get_products(self):
        base_url = 'http://111.231.88.85:38080/hysj_v2/wish_api/pro_infos?u_name=youran&time=1570762401&sign=60312dd9c60778b319b8a4b5a050b385'
        res = requests.post(base_url)
        ret = res.json()['result']
        for ele in ret:
            yield (ele['pid'], ele['pname'], ele['mid'], ele['mname'], ele['approved_date'], ele['is_promo'],
                   ele['is_verified'], ele['num_bought'], ele['num_entered'], ele['num_rating'], ele['rating'],
                   ele['gen_time'], ele['o_price'], ele['o_shipping'], ele['price'], ele['shipping'], ele['merchant'],
                   json.dumps(ele['c_ids']), ele['supplier_url'], json.dumps(ele['mer_tags']),
                   json.dumps(ele['pro_tags']), ele['is_hwc'],
                   ele['sales_week1'], ele['sales_week2'], ele['sales_growth'], ele['payment_week1'],
                   ele['payment_week2'], ele['wishs_sweek1'], ele['wishs_week2'], ele['wishs_growth'],
                   ele['hy_index'], ele['hot_flag'], ele['total_price'], ele['m_sales_week1'],
                   ele['rate_week1'], ele['daily_bought'], ele['status'], ele['is_pb'], ele['last_upd_date'],
                   ele['m_rating_count'], ele['m_status'], ele['feed_tile_text'], ele['view_flag'],
                   ele['view_rate1'], ele['view_rate_growth'], ele['interval_rating'], ele['last_modi_time'],
                   ele['max_num_bought'],
                   )

    def save_products(self, rows):
        sql = ('insert into proEngine.wish_products (pid, pname, mid, mname, approved_date, '
               'is_promo, is_verified, num_bought, num_entered, num_rating, rating, gen_time, '
               'o_price, o_shipping, price, shipping, merchant, c_ids, supplier_url, mer_tags, '
               'pro_tags, is_hwc, sales_week1, sales_week2, sales_growth, payment_week1, '
               'payment_week2, wishs_sweek1, wishs_week2, wishs_growth, hy_index, hot_flag, total_price, m_sales_week1,'
               ' rate_week1, daily_bought, `status`, is_pb, last_upd_date, m_rating_count, m_status, feed_tile_text, '
               'view_flag, view_rate1, view_rate_growth, interval_rating, last_modi_time, max_num_bought ) values ('
               '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
               '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)')
        self.warehouse_cur.executemany(sql, rows)
        self.warehouse_con.commit()

    def run(self):
        try:
            rows = self.get_products()
            self.save_products(rows)
            self.logger.info('success to get wish products from haiYing')
        except Exception as why:
            self.logger.error(f'fail to get wish products from haiYing cause of {why}')
        finally:
            self.close()


if __name__ == '__main__':
    worker = Worker()
    worker.run()
