#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-03-28 10:32
# Author: turpure


from src.services.base_service import BaseService
from src.services.tracking_api import Tracker
import concurrent.futures
from multiprocessing import Process, Queue
import datetime


class EbayTracker(BaseService):
    """
    get ebay order tracking info
    """
    def __init__(self, queue):
        super().__init__()
        self.queue = queue

    def get_track_no(self):
        sql = ("select  pt.nid as tradeId, expressNid, bw.name as expressName, trackNo, suffix, "
               "dateadd(hour,8, ordertime) as orderTime from p_trade(nolock) as pt"
               " LEFT JOIN b_logisticWay(nolock) as bw on pt.logicsWayNid= bw.nid"
               " where datediff(day,orderTime,getdate())<10 and expressNId in (5,42)"
               " and addressOwner='ebay' and trackno is not null")
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def save_trans(self, row):
        sql = ("insert into cache_express"
               "(suffix, tradeId,trackNo,expressName,orderTime,lastDate,lastDetail) "
               "values(%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY update "
               "lastDate=values(lastDate), lastDetail=values(lastDetail)")
        self.warehouse_cur.execute(sql,
                                   (row['suffix'], row['tradeId'], row['trackNo'], row['expressName'],
                                    row['orderTime'], row['lastDate'], row['lastDetail']))
        self.warehouse_con.commit()
        self.logger.info('success to fetch {} info'.format(row['tradeId']))

    def work(self, track_info):
        try:
            tracker = Tracker(track_info['trackNo'], track_info['expressNid'])
            ret = tracker.track()
            ret['tradeId'] = track_info['tradeId']
            ret['suffix'] = track_info['suffix']
            ret['orderTime'] = track_info['orderTime']
            ret['expressName'] = track_info['expressName']
            self.put(ret)
            # return ret

        except Exception as why:
            self.logger.error(why)

    def put(self, row):
        if not self.queue.full():
            self.queue.put(row)

    def concurrent_run(self):
        try:
            with concurrent.futures.ThreadPoolExecutor(8) as executor:
                future_task = {executor.submit(self.work, no): no for no in self.get_track_no()}
                for future in concurrent.futures.as_completed(future_task):
                    track_no = future_task[future]
                    try:
                        data = future.result()
                    except Exception as exc:
                        print('{}: exception-{}'.format(track_no, exc))
                    else:
                        self.save_trans(data)

        except Exception as why:
            self.logger.error(why)
        finally:
            self.close()

    def run(self):
        try:
            with concurrent.futures.ThreadPoolExecutor(8) as pool:
                pool.map(self.work, self.get_track_no())
        except Exception as why:
            self.logger.error(why)
        finally:
            self.close()


class Saver(BaseService):

    def __init__(self, queue, start):
        super().__init__()
        self.queue = queue
        self.start = start

    def save_trans(self):
        while True:
            sql = ("insert into cache_express"
                   "(suffix, tradeId,expressName,trackNo,orderTime,lastDate,lastDetail) "
                   "values(%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY update "
                   "lastDate=values(lastDate), lastDetail=values(lastDetail)")
            try:
                row = self.queue.get()
                self.warehouse_cur.execute(sql,
                                           (row['suffix'], row['tradeId'], row['expressName'], row['trackNo'],
                                           row['orderTime'], row['lastDate'], row['lastDetail']))
                self.warehouse_con.commit()
                self.logger.info('success to fetch {} info'.format(row['tradeId']))
            except Exception as why:
                self.logger.info(why)


def producer(qe):
    worker = EbayTracker(qe)
    worker.run()


def consumer(qe, start):
    worker = Saver(qe, start)
    worker.save_trans()
    worker.close()


def pro_con():
    queue = Queue()
    now = datetime.datetime.now()
    pp = Process(target=producer, args=(queue,))
    pc = Process(target=consumer, args=(queue, now))
    for p in [pp, pc]:
        p.start()
    pp.join()
    pc.terminate()


if __name__ == '__main__':
    queue = Queue()
    now = datetime.datetime.now()
    pp = Process(target=producer, args=(queue,))
    pc = Process(target=consumer, args=(queue, now))
    for p in [pp, pc]:
        p.start()
    pp.join()
    pc.terminate()














