#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-05-08 16:44
# Author: turpure

from src.services.base_service import BaseService


class Migration(BaseService):

    def get_detail(self):
        sql = ("select id , mid ,parentId ,proName ,description,tags,"
               "childId,color,proSize,quantity, price,msrPrice,shipping,"
               "shippingWeight,shippingTime,varMainImage,extra_image0 as  extraImage0 ,"
               "extra_image1 as  extraImage1 ,extra_image2 as  extraImage2 ,"
               "extra_image3 as  extraImage3 ,extra_image4 as  extraImage4 ,"
               "extra_image5 as  extraImage5 ,extra_image6 as  extraImage6 ,"
               "extra_image7 as  extraImage7 ,extra_image8 as  extraImage8 ,"
               "extra_image9 as  extraImage9 ,extra_image10 as  extraImage10 ,"
               "MainImage ,pySku from oa_data_mine_detail")

        self.cur.execute(sql)
        for row in self.cur:
            yield (row['id'], row['mid'], row['parentId'], row['proName'], row['description'], row['tags'], row['childId'], row['color'], row['proSize'], row['quantity'], row['price'], row['msrPrice'], row['shipping'], row['shippingWeight'], row['shippingTime'], row['varMainImage'], row['extraImage0'], row['extraImage1'], row['extraImage2'], row['extraImage3'], row['extraImage4'], row['extraImage5'], row['extraImage6'], row['extraImage7'], row['extraImage8'], row['extraImage9'], row['extraImage10'], row['MainImage'], row['pySku'], )

    def save(self, rows):
        sql = 'insert into proCenter.oa_dataMineDetail values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        self.warehouse_cur.executemany(sql, rows)
        self.warehouse_con.commit()

    def work(self):
        try:
            details = self.get_detail()
            self.save(details)
            self.logger.info('successful to migrate detail!')
        except Exception as why:
            self.logger.error('failed to migrate detail cause of {}'.format(why))
        finally:
            self.close()


if __name__ == '__main__':
    worker = Migration()
    worker.work()

