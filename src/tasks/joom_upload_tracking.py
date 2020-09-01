#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-06-10 14:20
# Author: turpure

from src.services.base_service import CommonService
import requests
import json


class Uploader(CommonService):

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.warehouse = 'mysql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.warehouse_cur = self.base_dao.get_cur(self.warehouse)
        self.warehouse_con = self.base_dao.get_connection(self.warehouse)

    def close(self):
        self.base_dao.close_cur(self.cur)
        self.base_dao.close_cur(self.warehouse_cur)

    def get_task(self):
        sql = "select tradeNid, trackNumber, expressName, isMerged from task_joom_tracking where ifnull(isDone,0)=0 and id=16"
        self.warehouse_cur.execute(sql)
        ret = self.warehouse_cur.fetchall()
        for row in ret:
            yield row

    def mark_shipped(self, row):
        sql = 'update {} set shippingMethod=1 where nid=%s'.format(row['tableName'])
        self.cur.execute(sql, (row['nid']))
        self.logger.info('success to mark {}'.format(row['nid']))
        self.con.commit()

    def cancel_shipped(self, row):
        sql = 'update {} set shippingMethod=0 where nid=%s'.format(row['tableName'])
        self.cur.execute(sql, (row['nid']))
        self.logger.info('success to cancel {}'.format(row['nid']))
        self.con.commit()

    def get_token(self, row):
        if row['isMerged'] == 0:
            sql = ("SELECT 'p_trade' as tableName, pt.nid, '{}' as trackNumber ,js.accessToken,"
                             "pt.ack,'{}' as provider FROM P_Trade(nolock) as pt"
                             " LEFT JOIN S_JoomSyncInfo as js on js.aliasname=pt.suffix WHERE pt.nid=%s "
                             "and accessToken is not null "
                             "union SELECT 'p_trade_his' as tableName, pt.nid, '{}' as trackNumber,js.accessToken,pt.ack,"
                             "'{}' as provider FROM P_Trade_his(nolock) as pt LEFT JOIN S_JoomSyncInfo as js "
                             "on js.aliasname=pt.suffix WHERE pt.nid= %s and accessToken is not null "
                             "union SELECT 'p_tradeun' as tableName, pt.nid, '{}' as trackNumber,js.accessToken,pt.ack,"
                             "'{}' as provider FROM P_Tradeun(nolock) as pt LEFT JOIN S_JoomSyncInfo as js "
                             "on js.aliasname=pt.suffix WHERE pt.nid= %s "
                             "and accessToken is not null").format(
                row['trackNumber'], row['expressName'],
                row['trackNumber'], row['expressName'],
                row['trackNumber'], row['expressName'])

        else:
            sql = ("SELECT 'p_trade' as tableName,pt.nid, '{}'  as trackNumber,"
                          "js.accessToken,pb.ack,'{}' as provider FROM  P_Trade(nolock) as pt"
                          " LEFT JOIN S_JoomSyncInfo(nolock) as js "
                          "on js.aliasname=pt.suffix LEFT JOIN p_trade_b(nolock) as pb "
                          "on pb.MergeBillID=pt.nid WHERE pt.nid=%s and accessToken is not null"
                          " union SELECT 'p_trade_his' as tableName, pt.nid, '{}'  as trackNumber,js.accessToken,"
                          "pb.ack,'{}' as provider FROM  P_Trade_his(nolock) as pt "
                          "LEFT JOIN S_JoomSyncInfo(nolock) as js on js.aliasname=pt.suffix "
                          "LEFT JOIN p_trade_b(nolock)  as pb on  pb.MergeBillID=pt.nid "
                          "WHERE pt.nid=%s and accessToken is not null  "
                          " union SELECT 'p_tradeun' as tableName,pt.nid, '{}'  as trackNumber,"
                          "js.accessToken,pb.ack,'{}' as provider FROM  "
                          "P_Tradeun(nolock) as pt  LEFT JOIN S_JoomSyncInfo(nolock) as js "
                          "on js.aliasname=pt.suffix LEFT JOIN p_trade_b(nolock)  as pb on  "
                          "pb.MergeBillID=pt.nid WHERE pt.nid=%s and accessToken is not null").format(
                row['trackNumber'], row['expressName'],
                row['trackNumber'], row['expressName'],
                row['trackNumber'], row['expressName'])
        self.cur.execute(sql, (row['tradeNid'], row['tradeNid'], row['tradeNid']))
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def ship(self, row):
        token = row['accessToken']
        headers = {'content-type': 'application/json', 'Authorization': 'Bearer ' + token}
        order_id = row['ack']
        track_number = row['trackNumber']
        provider = row['provider']
        base_url = 'https://api-merchant.joom.com/api/v2/order/fulfill-one'
        payload = {
            "id": order_id,
            "tracking_provider": provider,
            "tracking_number": track_number
        }
        try:
            response = requests.post(base_url, params=payload, headers=headers, timeout=20)
            ret = json.loads(response.text)
            if ret['code'] == 0:
                self.logger.info('success to ship {} saying {}'.format(json.dumps(row), ret))
                return True
            else:
                self.logger.info('fail to ship {} saying {}'.format(json.dumps(row), ret))
        except Exception as e:
            self.logger.error('fail to ship {} saying {}'.format(json.dumps(row), e))

    def update_tracking(self, row):
        token = row['accessToken']
        order_id = row['ack']
        track_number = row['trackNumber']
        provider = row['provider']
        base_url = 'https://api-merchant.joom.com/api/v2/order/modify-tracking'
        payload = {
            "access_token": token,
            "id": order_id,
            "tracking_number": track_number,
            "tracking_provider": provider
        }
        headers = {'content-type': 'application/json'}
        try:
            response = requests.post(base_url, params=payload, headers=headers, timeout=20)
            ret = json.loads(response.text)
            if ret['code'] == 0:
                self.logger.info('success to update {} saying {}'.format(json.dumps(row), ret))
                return True
            else:
                self.logger.info('fail to update {} saying {}'.format(json.dumps(row), ret))

        except Exception as e:
            self.logger.error('error to update {} cause of {}'.format(json.dumps(row), e))
            return False

    def upload(self, row):
        try:
            ret = self.update_tracking(row)
            if not ret:
                if self.ship(row):
                    self.mark_shipped(row)
            else:
                self.mark_shipped(row)
        except Exception as e:
            self.logger.error('fail to upload {} cause of {}'.format(row['nid'], e))

    def work(self):
        try:
            for ele in self.get_task():
                for row in self.get_token(ele):
                    self.upload(row)

        except Exception as why:
            self.logger.error('fail to upload tracking number to joom cause of {}'.format(why))
        finally:
            self.close()


if __name__ == '__main__':
    worker = Uploader()
    worker.work()

