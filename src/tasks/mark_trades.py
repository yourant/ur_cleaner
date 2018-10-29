#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-19 13:51
# Author: turpure

import datetime
import re
from src.services import db, log
from concurrent.futures import ThreadPoolExecutor


class Marker(object):
    """
    mark trades out of stock
    """
    def __init__(self):
        self.con = db.Mssql().connection
        self.logger = log.SysLogger().log
        self.cur = self.con.cursor(as_dict=True)

    def transport_exception_trades(self, trade_info):
        max_bill_code_query = "P_S_CodeRuleGet 130,''"
        exception_trade_handler = "P_ExceptionTradeToException %s, 3 ,'取消订单', '%s'"
        marked_days = self.calculate_mark_day(trade_info['memo'])
        if marked_days >= 5:
            try:
                self.cur.execute(max_bill_code_query)
                code_ret = self.cur.fetchone()
                max_bill_code = code_ret['MaxBillCode']
                self.cur.execute(exception_trade_handler % (trade_info['nid'], max_bill_code))
                self.logger.info('transporting %s' % trade_info['nid'])
            except Exception as e:
                self.logger.error('%s while fetching the exception trades' % e)
        else:
            self.logger.info('not need to transport %s' % trade_info['nid'])

    def calculate_mark_day(self, memo):
        try:
            year = str(datetime.datetime.now())[:5]
            lasted_marked_day = (year + re.findall('\d\d-\d\d', memo)[-1]).split('-')
            mark_day = datetime.datetime(int(lasted_marked_day[0]),
                                         int(lasted_marked_day[1]),
                                         int(lasted_marked_day[2]))

        except Exception as e:
            self.logger.error('%s while calculate the marked day' % e)
            mark_day = datetime.datetime.now()
        today = datetime.datetime.now()
        delta_day = (today - mark_day).days
        if delta_day < 0:
            delta_day = 5
        return delta_day

    def prepare_to_mark(self):
        trades_to_mark_sql = "www_outOfStock_sku '7','春节放假,清仓,停产,停售,线下清仓,线上清仓,线上清仓50P,线上清仓100P'"
        empty_mark_sql = "update p_tradeUn set reasonCode = '', memo = %s where nid = %s"
        pattern = '不采购: .*;'
        today = str(datetime.datetime.now())[5:10]
        self.cur.execute(trades_to_mark_sql)
        trades_to_mark = self.cur.fetchall()
        ret_trades = {}
        for tra in trades_to_mark:
            memo = tra['memo']
            origin_memo = re.sub(pattern, '', memo)
            if tra['which'] == 'pre':
                self.cur.execute(empty_mark_sql, (origin_memo, tra['tradeNid']))
                self.con.commit()
                self.logger.info('emptying %s', tra['tradeNid'])
            else:
                mark_memo = '不采购: ' + tra['purchaser'] + today + ':' + tra['sku'] + tra['goodsSkuStatus'] + ';'
                trade = {
                    'tradeNid': tra['tradeNid'],
                    'mark_memo': mark_memo,
                    'origin_memo': origin_memo,
                    'reasonCode': tra['howPur']
                }
                if tra['tradeNid'] in ret_trades:
                    ret_trades[tra['tradeNid']]['mark_memo'] += mark_memo
                else:
                    ret_trades[tra['tradeNid']] = trade

        return ret_trades

    def handle_exception_trades_trans(self):
        """
        if delay days of the trade is equal or greater than
        7 and being marked days is equal or greater than 5,
        then the trade should be transported to exception trades
        """
        exception_sql = "select nid,reasoncode,memo,DATEDIFF(day, dateadd(hour,8,ordertime), GETDATE())" \
                        " as deltaday from p_tradeun " \
                        "where (reasoncode like '%不采购%' or reasoncode like '%春节%') " \
                        "and PROTECTIONELIGIBILITYTYPE='缺货订单' " \
                        "and DATEDIFF(day, dateadd(hour,8,ordertime), GETDATE())>=7"

        self.cur.execute(exception_sql)
        exception_trades = self.cur.fetchall()
        for trade in exception_trades:
            self.transport_exception_trades(trade)

    def mark_trades_trans(self):
        update_memo_sql = "update p_tradeUn set memo = '%s', reasonCode = '%s' where nid = %s"
        trades_to_mark = self.prepare_to_mark()
        for mar in trades_to_mark.values():
            new_memo = mar['origin_memo'] + mar['mark_memo']
            self.cur.execute(update_memo_sql % (new_memo, mar['reasonCode'], mar['tradeNid']))
            self.con.commit()
            self.logger.info('marking %s', mar['tradeNid'])

    def run(self):
        try:
            self.handle_exception_trades_trans()
            self.mark_trades_trans()
        except Exception as e:
            self.logger.error(e)
        finally:
            self.cur.close()
            self.con.close()


if __name__ == '__main__':
    worker = Marker()
    worker.run()
