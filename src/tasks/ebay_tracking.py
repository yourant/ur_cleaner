#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-03-28 10:32
# Author: turpure


from src.services.base_service import BaseService
from src.services.tracking_api import Tracker
from multiprocessing import Pool


class EbayTracker(BaseService):
    """
    get ebay order tracking info
    """

    def get_track_no(self):
        sql = ("select nid as tradeId, trackNo, suffix, dateadd(hour,8, ordertime) as orderTime from p_trade(nolock)"
               " where datediff(day,orderTime,getdate())=10 and expressNId in (5,42)"
               " and trackno is not null and addressOwner='ebay'")
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def save_trans(self, row):
        sql = ("insert into cache_express"
               "(suffix, tradeId,trackNo,orderTime,lastDate,lastDetail) "
               "values(%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY update "
               "lastDate=values(lastDate), lastDetail=values(lastDetail)")
        self.warehouse_cur.execute(sql,
                                   (row['suffix'], row['tradeId'], row['trackNo'],
                                   row['orderTime'], row['lastDate'], row['lastDetail']))
        self.warehouse_con.commit()

    def work(self, track_info):
        try:
            tracker = Tracker(track_info['trackNo'])
            ret = tracker.track()
            ret['tradeId'] = track_info['tradeId']
            ret['suffix'] = track_info['suffix']
            ret['orderTime'] = track_info['orderTime']
            self.save_trans(ret)
            self.logger.info('success to fetch {} info'.format(track_info['tradeId']))

        except Exception as why:
            self.logger.error(why)

    def run(self):
        try:
            for row in self.get_track_no():
                self.work(row)
        except Exception as why:
            self.logger.error(why)


if __name__ == '__main__':
    worker = EbayTracker()
    worker.run()







