#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-11-28 13:15
# Author: turpure

from pymongo import MongoClient


class Worker(object):

    def __init__(self):
        self.mongo = MongoClient('192.168.0.150', 27017)
        self.mongodb = self.mongo['product_engine']
        self.col = self.mongodb['ebay_recommended_product']

    def get_reason(self):
        ret = self.col.find()
        for row in ret:
            ele = {'itemId': row['itemId'], 'reasons': row.get('refuse', '')}
            yield ele

    def parse(self, row):
        reason_map = {
            "1：产品重复": "1: 重复",
            "2：产品侵权": "2: 侵权",
            "3：产品不好运输": "3: 不好运输",
            "4：销量不好": "4: 销量不好",
            "5：找不到货源": "5: 找不到货源",
            "6：价格没有优势": "6: 价格没优势",
            "7：产品评价低": "7: 评分低"
        }
        ren = row['reasons']
        if ren:
            ret = {}
            for key, value in ren.items():
                if value in reason_map:
                    ret[key] = reason_map[value]
                elif value.startswith('8：其他'):
                    ret[key] = value.replace('8：其他', '8: 其他')
                else:
                    ret[key] = value
            row['reasons'] = ret
            print(row)
            return row

    def save(self, row):
        self.col.find_one_and_update({"itemId": row['itemId']}, {'$set': {"refuse": row['reasons']}})
        print(f'success to update {row["itemId"]}')

    def run(self):
        try:
            reasons = self.get_reason()
            for row in reasons:
                ele = self.parse(row)
                if ele:
                    print(ele)
                    # self.save(ele)
        except Exception as why:
            print(f'fail to update refused reason because of {why}')

        finally:
            self.mongo.close()


if __name__ == '__main__':
    worker = Worker()
    worker.run()
