#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-05-14 15:07
# Author: turpure

import os
from src.services.base_service import CommonService
import re
import datetime
import time


class Checker(CommonService):
    """
    smt跟踪号超时标记
    1. 缺货单和4px 在其他备注里面加【跟踪号超时】
    2. 未拣货和未核单 在内部便签里面追加【跟踪号超时】
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.warehouse = 'mysql'
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.warehouse_cur = self.base_dao.get_cur(self.warehouse)
        self.warehouse_con = self.base_dao.get_connection(self.warehouse)
        self.today = str(datetime.datetime.now())[:10]

    def close(self):
        self.base_dao.close_cur(self.cur)
        self.base_dao.close_cur(self.warehouse_cur)

    def get_trades_out_of_stock(self):
        """
        缺货单
        :return:
        """
        sql = ("select  nid, logs,addressOwner,name  from (select pt.nid, logs,addressOwner,name, "
               "row_number() over (partition by pt.nid order by plog.nid desc) as rn from p_tradeun(nolock) as pt"
               " LEFT JOIN P_TradeLogs(nolock) as plog on cast(pt.Nid as varchar(20)) = plog.tradenid "
               "LEFT JOIN b_logisticWay(nolock) as bw on pt.logicsWayNid = bw.nid "
               "where PROTECTIONELIGIBILITYTYPE='缺货订单'  and pt.trackNo is not null "
               "and (plog.logs like '%预获取转单号成功%' or plog.logs like '%跟踪号成功,跟踪号%'"
               " or plog.logs like '%提交订单成功!   跟踪号:%') ) as td where rn=1")
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_trades_unchecked_unpicked(self):
        """
        未拣货:filterFlag=20
        未核单:filterFlag=22
        :return:
        """
        sql = ("select  nid, logs,nid as lognid,addressOwner,name from (  "
                "select pt.nid, plog.logs,plog.nid as lognid,pt.addressOwner,bw.name, row_number() over (partition by pt.nid order by plog.nid desc) as rn from  "
                "(select ordertime,nid, addressOwner, trackNo,logicsWayNid from p_trade(nolock)  where addressOwner ='aliexpress' and	FilterFlag in(22,20) ) as pt   "
                " LEFT JOIN P_TradeLogs(nolock) as plog on cast(pt.Nid as varchar(20)) = plog.tradenid   "
                "LEFT join P_TradeLogs(nolock) as tlog on   tlog.nid = plog.nid  "
                " LEFT JOIN b_logisticWay(nolock) as bw on pt.logicsWayNid = bw.nid 						  "
                " where 	pt.trackNo is not null   "
                " and (plog.logs like '%预获取转单号成功%' or plog.logs like '%跟踪号成功,跟踪号%' or plog.logs like '%提交订单成功!   跟踪号:%')  "
                ") td  "
                "where td.rn =1 " )
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

    def parse(self, trades):
        out = {}
        for row in trades:
            date = re.search(r'\d{4}-\d{2}-\d{2}', row['logs']).group(0)
            if row['nid'] not in out:
                out[row['nid']] = {'express': row['name'], 'date': date, 'addressOwner': row['addressOwner']}
            else:
                if out[row['nid']]['date'] < date:
                    out[row['nid']] = {'express': row['name'], 'date': date, 'addressOwner': row['addressOwner']}
        return out

    def mark_out_of_stock_trades(self, nid):
        try:
            sql = 'select count(*) as ret from CG_OutofStock_Total where tradeNid=%s'
            self.cur.execute(sql, (nid,))
            ret = self.cur.fetchone()
            is_existed = ret['ret']
            if is_existed == 0:
                sql = 'insert into CG_OutofStock_Total(TradeNid, PrintMemoTotal) values (%s, %s)'
                self.cur.execute(sql, (nid, '跟踪号超时'))
                self.con.commit()
        except Exception as why:
            self.logger.error(f'fail to mark-express of p_tradeun {nid} cause of {why} ')

    def unmark_out_of_stock_trades(self, nid):
        try:
            sql = 'delete from CG_OutofStock_Total where tradeNid=%s and PrintMemoTotal=%s'
            self.cur.execute(sql, (nid, '跟踪号超时'))
            self.con.commit()
        except Exception as why:
            self.logger.error(f'fail to  unmark-express of p_tradeun {nid} cause of {why} ')

    def mark_unchecked_unpicked_trades(self, nid):
        """
        内部标签里面追加【跟踪号超时】
        :param nid:
        :return:
        """
        try:
            search_sql = 'select memo from p_trade(nolock) where nid = %s'
            self.cur.execute(search_sql, (nid,))
            ret = self.cur.fetchone()
            if ret:
                memo = ret['memo']
                memo = memo.replace(';跟踪号超时', '') + ';跟踪号超时'
            else:
                memo = ';跟踪号超时'
            sql = "update p_trade set memo =   %s where nid = %s "
            self.cur.execute(sql, (memo, nid))
            self.con.commit()
        except Exception as why:
            self.logger.error(f'fail to  mark-express of p_trade {nid} cause of {why} ')

    def unmark_unchecked_unpicked_trades(self, nid):
        """
        内部标签里面取消【跟踪号超时】
        :param nid:
        :return:
        """
        try:
            search_sql = 'select memo from p_trade(nolock) where nid = %s'
            self.cur.execute(search_sql, (nid,))
            ret = self.cur.fetchone()
            if ret:
                memo = ret['memo']
                memo = memo.replace(';跟踪号超时', '')
            else:
                memo = ''

            set_sql = "update p_trade set memo = %s where nid = %s "
            self.cur.execute(set_sql, (memo, nid))
            self.con.commit()
        except Exception as why:
            self.logger.error(f'fail to unmark-express of p_trade {nid} cause of {why} ')

    def mark_out_of_stock_trades_trans(self, express_info):
        """
        标记缺货单事务
        :param express_info:
        :return:
        """
        trades = self.get_trades_out_of_stock()
        trades = self.parse(trades)
        today = datetime.datetime.today()
        for row in trades.items():
            nid = row[0]
            date = row[1]['date']
            express = row[1]['express']
            if express in express_info:
                if row[1]['addressOwner'] == 'aliexpress':
                    if (today - datetime.datetime.strptime(date, '%Y-%m-%d')).days >= express_info[express] + 2:
                        self.mark_out_of_stock_trades(nid)
                    else:
                        self.unmark_out_of_stock_trades(nid)
                else:
                    if (today - datetime.datetime.strptime(date, '%Y-%m-%d')).days >= express_info[express]:
                        self.mark_out_of_stock_trades(nid)
                    else:
                        self.unmark_out_of_stock_trades(nid)

    def mark_unchecked_unpicked_trades_trans(self, express_info):
        """
        标记未拣货，未核单事务
        :param express_info: 
        :return: 
        """""
        trades = self.get_trades_unchecked_unpicked()
        trades = self.parse(trades)
        today = datetime.datetime.today()
        for row in trades.items():
            nid = row[0]
            date = row[1]['date']
            express = row[1]['express']
            if express in express_info:
                if row[1]['addressOwner'] == 'aliexpress':
                    if (today - datetime.datetime.strptime(date, '%Y-%m-%d')).days >= express_info[express] + 2:
                        self.mark_unchecked_unpicked_trades(nid)
                    else:
                        self.unmark_unchecked_unpicked_trades(nid)
                else:
                    if (today - datetime.datetime.strptime(date, '%Y-%m-%d')).days >= express_info[express]:
                        self.mark_unchecked_unpicked_trades(nid)
                    else:
                        self.unmark_unchecked_unpicked_trades(nid)

    def run(self):
        begin_time = time.time()
        try:
            express_info = self.get_express()

            # 标记缺货单
            self.mark_out_of_stock_trades_trans(express_info)

            # 标记未核单和未拣货
            self.mark_unchecked_unpicked_trades_trans(express_info)

        except Exception as why:
            self.logger.error(why)
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()
            self.logger.info(f'it takes {time.time() - begin_time}')


if __name__ == '__main__':
    worker = Checker()
    worker.run()


