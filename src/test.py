#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-20 15:56
# Author: turpure

from src.services.base_service import BaseService
import datetime
from pymongo import MongoClient


class AliSync(BaseService):
    """
    check purchased orders
    """

    def __init__(self):
        super().__init__()
        self.mongo = MongoClient('192.168.0.150', 27017)
        self.mongodb = self.mongo['ur_cleaner']
        self.col = self.mongodb['delete_tupianku_tasks']

    def insert(self, row):
        sql = (
            'insert into y_fee(notename,fee_type,total,currency_code,fee_time,batchId,description,itemId,memo,'
            'transactionId,orderId,recordId) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) '
        )
        try:
            self.cur.executemany(sql, row)
            self.con.commit()
        except Exception as e:
            self.logger.error("%s while trying to save data" % e)

    def get_ebay_token(self):
        sql = ("SELECT  NoteName AS suffix,EuSellerID AS storeName, MIN(EbayTOKEN) AS token "
               "FROM [dbo].[S_PalSyncInfo] WHERE SyncEbayEnable=1 "
               "and notename in (select dictionaryName from B_Dictionary "
               "where  CategoryID=12 and FitCode ='eBay' and used = 0) "
               "GROUP BY NoteName,EuSellerID ORDER BY NoteName ")
        self.cur.executemany(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield row

    def get_batch_id(self):
        # sql = ("select max(batchId) batchId from y_fee"
        #        " where notename in "
        #        "(select DictionaryName from B_Dictionary nolock "
        #        "where CategoryID=12 and FitCode ='eBay')"
        #        )
        sql = ("SELECT  goodsCode FROM [dbo].[B_Goods]  " +
               " WHERE  isnull(goodsCode,'') IN ('UK-A0008' ,'UK-A0012' ,'UK-A0042' ,'UK-A0051' ,'UK-A0052' ,'UK-A0056' ,'UK-A0069' ,'UK-A0115' ,'UK-A0139' ,'UK-A0145' ,'UK-A0186' ,'UK-A0187' ,'UK-A0190' ,'UK-A0221' ,'UK-A0240' ,'UK-A0241' ,'UK-A0243' ,'UK-A0299' ,'UK-A0305' ,'UK-A0325' ,'UK-A0333' ,'UK-A0339' ,'UK-A0347' ,'UK-A0348' ,'UK-A0419' ,'UK-A0421' ,'UK-A0434' ,'UK-A0448' ,'UK-A0449' ,'UK-A0475' ,'UK-A0504' ,'UK-A0509' ,'UK-A0516' ,'UK-A0544' ,'UK-A0550' ,'UK-A0551' ,'UK-A0570' ,'UK-A0574' ,'UK-A0620' ,'UK-A0647' ,'UK-A0705' ,'UK-L0004' ,'UK-L0009' ,'UK-L0010' ,'UK-L0012' ,'UK-L0014' ,'UK-L0020' ,'UK-L0022' ,'UK-L0030' ,'UK-L0059' ,'UK-L0060' ,'UK-L0069' ,'UK-L0082' ,'UK-L0095' ,'UK-L0099' ,'UK-L0105' ,'UK-L0107' ,'UK-L0109' ,'UK-L0120' ,'UK-L0128' ,'UK-L0133' ,'UK-L0142' ,'UK-L0145' ,'UK-L0146' ,'UK-L0147' ,'UK-L0153' ,'UK-L0159' ,'UK-L0160' ,'UK-L0161' ,'UK-L0162' ,'UK-L0166' ,'UK-L0170' ,'UK-L0171' ,'UK-L0178' ,'UK-L0179' ,'UK-L0183' ,'UK-L0185' ,'UK-L0193' ,'UK-L0197' ,'UK-L0198' ,'UK-L0200' ,'UK-L0201' ,'UK-L0203' ,'UK-L0210' ,'UK-L0212' ,'UK-L0214' ,'UK-L0215' ,'UK-L0216' ,'UK-L0217' ,'UK-L0218' ,'UK-L0220' ,'UK-L0222' ,'UK-L0225' ,'UK-L0226' ,'UK-L0227' ,'UK-L0243' ,'UK-L0245' ,'UK-L0248' ,'UK-L0249' ,'UK-L0251' ,'UK-L0255' ,'UK-L0258' ,'UK-L0259' ,'UK-L0261' ,'UK-L0262' ,'UK-L0265' ,'UK-L0288' ,'UK-L0290' ,'UK-L0291' ,'UK-L0292' ,'UK-L0297' ,'UK-L0298' ,'UK-L0305' ,'UK-L0308' ,'UK-L0310' ,'UK-L0313' ,'UK-L0315' ,'UK-L0316' ,'UK-L0320' ,'UK-L0325' ,'UK-L0326' ,'UK-L0328' ,'UK-L0331' ,'UK-L0334' ,'UK-L0341' ,'UK-L0346' ,'UK-L0347' ,'UK-L0354' ,'UK-L0355' ,'UK-L0357' ,'UK-L0363' ,'UK-L0364' ,'UK-L0365' ,'UK-L0367' ,'UK-L0372' ,'UK-L0376' ,'UK-L0379' ,'UK-L0380' ,'UK-L0383' ,'UK-L0384' ,'UK-L0385' ,'UK-L0386' ,'UK-L0390' ,'UK-L0392' ,'UK-L0405' ,'UK-L0422' ,'UK-L0425' ,'UK-L0431' ,'UK-L0433' ,'UK-L0441' ,'UK-L0443' ,'UK-L0470' ,'UK-L0474')"
               )
        try:
            self.cur.execute(sql)
            goods_list = self.cur.fetchall()
            for row in goods_list:
                row['_id'] = row['goodsCode']
                row['tupianku1'] = 0
                row['tupianku2'] = 0
                self.col.update_one({'_id': row['_id']}, {"$set": row}, upsert=True)

        except Exception as why:
            self.logger.error('fail to get max batchId cause of {}'.format(why))

    def run(self):
        try:
            # for i in range(33):
            #     begin = str(datetime.datetime.strptime('2020-08-01', '%Y-%m-%d') + datetime.timedelta(days=i))[:10]
            #     # print(begin)
            #     rows = self.get_data(begin)
            #     for row in rows:
            #         # print(row)
            #         col2.update_one({'recordId': row['recordId']}, {"$set": row}, upsert=True)
            #     self.logger.info(f'success to sync data in {begin}')
            self.get_batch_id()
        except Exception as e:
            self.logger(e)
        finally:
            self.close()


if __name__ == '__main__':
    sync = AliSync()
    sync.run()
