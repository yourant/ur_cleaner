#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-05-14 15:07
# Author: turpure

from src.services.base_service import BaseService
import re
import datetime


class Checker(BaseService):

    def get_trades(self):
        sql = ("select pt.nid, logs,name from p_tradeun(nolock) as pt"
               " LEFT JOIN P_TradeLogs(nolock) as plog on cast(pt.Nid as varchar(20)) = plog.tradenid "
               "LEFT JOIN b_logisticWay as bw on pt.logicsWayNid = bw.nid "
               "where PROTECTIONELIGIBILITYTYPE='缺货订单'  and (pt.trackNo is not null "
               "and plog.logs like '%预获取转单号成功%' or plog.logs like '%跟踪号成功,跟踪号%'"
               " or plog.logs like '%提交订单成功!   跟踪号:%')")
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_express(self):
        sql = "select expressName, `days` from urTools.express_deadline"
        self.warehouse_cur.execute(sql)
        ret = self.warehouse_cur.fetchall()
        express = {}
        for row in ret:
            express[row['expressName']] = row['days']
        return express

    def parse(self):
        trades = self.get_trades()
        out = {}
        for row in trades:
            date = re.search(r'\d{4}-\d{2}-\d{2}', row['logs']).group(0)
            if row['nid'] not in out:
                out[row['nid']] = {'express': row['name'], 'date': date}
            else:
                if out[row['nid']]['date'] < date:
                    out[row['nid']] = {'express': row['name'], 'date': date}
        return out

    def mark(self, nid):
        try:
            sql = 'select count(*) as ret from CG_OutofStock_Total where tradeNid=%s'
            self.cur.execute(sql, (nid,))
            ret = self.cur.fetchone()
            is_existed = ret['ret']
            if is_existed == 0:
                sql = 'insert into CG_OutofStock_Total(TradeNid, PrintMemoTotal) values (%s, %s)'
                self.cur.execute(sql, (nid, '跟踪号超时'))
                self.con.commit()
                self.logger.info('mark-express {}'.format(nid))
        except Exception as why:
            self.logger.error(why)

    def unmark(self, nid):
        try:
            sql = 'delete from CG_OutofStock_Total where tradeNid=%s and PrintMemoTotal=%s'
            self.cur.execute(sql, (nid, '跟踪号超时'))
            self.con.commit()
            self.logger.info('unmark-express {}'.format(nid))
        except Exception as why:
            self.logger.error(why)

    def check(self, express_info):
        trades = self.parse()
        today = datetime.datetime.today()
        for row in trades.items():
            nid = row[0]
            date = row[1]['date']
            express = row[1]['express']
            if express in express_info:
                if (today - datetime.datetime.strptime(date, '%Y-%m-%d')).days >= express_info[express]:
                    self.mark(nid)
                else:
                    self.unmark(nid)

    def run(self):
        try:
            express_info = self.get_express()
            self.check(express_info)
        except Exception as why:
            self.logger.error(why)
        finally:
            self.close()


if __name__ == '__main__':
    worker = Checker()
    worker.run()


