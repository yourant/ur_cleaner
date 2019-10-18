#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-10-14 13:25
# Author: turpure

from src.services.base_service import BaseService



class Worker(BaseService):

    def get_data(self):
        sql = ('SELECT jp.productId,jp.storeName, jp.productName, jp.price, jp.mainImage,jp.rating,jp.storeId, '
                'jl.reviewsCount, jl.procreatedDate as publishedDate from proCenter.joom_storeProduct as jp LEFT JOIN proCenter.joom_product as jl '
                'on jp.productid = jl.productid where jl.procreatedDate is not null limit 1000')

        self.warehouse_cur.execute(sql)
        ret = self.warehouse_cur.fetchall()
        for row in ret:
            yield (row.get('productId', ''), row.get('cateId', ''), row.get('productName', ''), row.get('price', 0),
                   row.get('msrPrice', 0), row.get('mainImage', ''), row.get('rating', 0),
                   row.get('publishedDate', 'now()'), row.get('rate_week1', 0), row.get('rate_week2', 0),
                   row.get('interval_rating', 0), row.get('hot_index', 0), row.get('hot_flag', 0),
                   row.get('lastModifyTime', '1990-01-01'), row.get('reviewsCount', 0), row.get('storeId', ''))

    def save(self, rows):
        sql = ('insert into proEngine.joom_products (productId , cateId , productName , price , msrPrice , mainImage , '
               'rating , publishedDate , rate_week1 , rate_week2 , interval_rating , hot_index , hot_flag , '
               'lastModifyTime , reviewsCount , storeId ) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)')
        self.warehouse_cur.executemany(sql, rows)
        self.warehouse_con.commit()

    def run(self):
        try:
            rows = self.get_data()
            self.save(rows)
            self.logger.info('success to fetch joom products')
        except Exception as why:
            self.logger.error(f'fail to fetch joom products cause of {why}')
        finally:
            self.close()


if __name__ == '__main__':
    worker = Worker()
    worker.run()

