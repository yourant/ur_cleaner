#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-05-09 13:07
# Author: turpure


from src.services.base_service import BaseService


class Converter(BaseService):

    def get_status(self):
        sql = "select id, dictionaryName  from  proCenter.oa_goodsinfo where ifnull(dictionaryName,'') != ''"
        self.warehouse_cur.execute(sql)
        ret = self.warehouse_cur.fetchall()
        for row in ret:
            yield row

    def parse(self, row):
        status = row['dictionaryName']
        if status:
            ret = []
            for ele in status.split(','):
                if ele:
                    ret.append(ele)
            ret.sort()
            row['name'] = ','.join(ret)
            return row

    def update(self, row):
        sql = 'update proCenter.oa_goodsinfo set dictionaryName =%s where id = %s'
        self.warehouse_cur.execute(sql, (row['name'], row['id']))
        self.warehouse_con.commit()
        self.logger.info('updating {}'.format(row['id']))

    def run(self):
        try:
            for row in self.get_status():
                ret = self.parse(row)
                if ret:
                    self.update(ret)
        except Exception as why:
            self.logger.error('failed to update cause of {}'.format(why))
        finally:
            self.close()


if __name__ == '__main__':
    worker = Converter()
    worker.run()



