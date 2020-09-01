#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-11-26 14:55
# Author: turpure


from src.services.base_service import CommonService
import re


class LogAnalysis(CommonService):
    """
    analysis exception logs
    """

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

    def load_logs(self):
        sql = ("select pt.nid,ptlog.logs, 'p_trade' as tableName from p_trade  as pt with(nolock) "
               'LEFT JOIN p_tradelogs as ptlog with(nolock)  on '
               'cast(pt.nid as varchar(20)) =ptlog.tradenid  '
               'where pt.ordertime BETWEEN dateadd(day,-2,getdate()) and getdate() and '
               "logs like '%地址修改%' UNION "
               "select pt.nid,ptlog.logs, 'P_tradeun' as tableName from p_tradeun  as pt with(nolock) "
               'LEFT JOIN p_tradelogs as ptlog with(nolock)  on '
               'cast(pt.nid as varchar(20)) =ptlog.tradenid  '
               'where pt.ordertime BETWEEN dateadd(day,-2,getdate()) and getdate() and '
               "logs like '%地址修改%' "
               )

        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def logs_handler(self, ele):
        try:

            change = re.sub(r'[ ]', '', re.sub(r'.*\d+.*:', '', ele['logs'])).split('\r\n')
            ele['shipToName'] = re.sub(r'[ ]', '', change[0]).lower()
            ele['editor'] = re.search(r'([\u4e00-\u9fa5]+)', ele['logs']).group()
            if re.match(r'[A-Za-z]', change[-1]):
                ele['shipToZip'] = re.sub(r'[ ]', '', change[-3]).lower()
            else:
                ele['shipToZip'] = re.sub(r'[ ]', '', change[-2]).lower()
            ele.pop('logs')
            return ele
        except Exception as why:
            self.logger.error(why)

    def load_now_info(self, ele):

        sql = 'select shipToName,shipToZip from {} with(nolock) where nid = %s'
        self.cur.execute(sql.format(ele['tableName']), (ele['nid'],))
        ret = self.cur.fetchone()
        ret['shipToName'] = re.sub(r'[ ]', '', ret['shipToName']).lower()
        ret['shipToZip'] = re.sub(r'[ ]', '', ret['shipToZip']).lower()
        return ret

    def save(self, exception_log):
        sql = ('insert into exceptionEdition(editor,shipToName,shipToZip,tableName,tradeNid,createdTime)'
               ' values (%s,%s,%s,%s,%s,now())')

        try:
            self.warehouse_cur.execute(sql, (exception_log['editor'],
                                             exception_log['shipToName'],
                                             exception_log['shipToZip'],
                                             exception_log['tableName'],
                                             exception_log['nid']))
            self.warehouse_con.comit()
        except Exception as e:
            self.logger.error(e)

    def work(self):
        try:
            for log in self.load_logs():
                old_info = self.logs_handler(log)
                now_info = self.load_now_info(log)
                if old_info['shipToName'] != now_info['shipToName'] and old_info['shipToZip'] != now_info['shipToZip']:
                    self.logger.info('find exception edition {}'.format(old_info))
                    self.save(old_info)

        except Exception as why:
            self.logger.error(why)
        finally:
            self.close()


if __name__ == '__main__':
    worker = LogAnalysis()
    worker.work()



