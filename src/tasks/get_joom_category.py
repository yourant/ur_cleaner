#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-09-16 11:20
# Author: turpure


from src.services.base_service import BaseService
import requests


class JoomCategoryFetcher(BaseService):

    def __init__(self):
        super().__init__()
        self.base_url = ('https://api.joom.com/1.1/categoriesHierarchy?'
                         'currency=USD&language=en-US&levels=1&_=jxo1mc94&categoryId={}')

        self.headers = {'Authorization': 'Bearer SEV0001MTU0NjY1Mzc2M3xTSU9XdUJFbFU4NDJ5VHVndk8tV3ROem8yYVFCV0QtYjE2aTBDM3FNLWZkbVFyX01aTFJUek05REJZUVZnWVNmOE5TanlCWXhYRk84MWFINHZDTE5UVUJGb0ZSTmFWLXlkZlRCem9YRVg4R21GSEEwVHNQeHJIUWZKMmJ5dWd2VmpKNkZ4Q0V6VS1JdF9EZzF1UGtyb1NzcVQ5VDlQLTRwNnJ4Nl9yaHZTUkEzUmRfZUI0ZFB1TGxXejFFTkNzNm1PUzZoY1BScXI1YVhEWGlWdmVwODJIOVhxTnlZcGYxSVdDQzJXY1RTQjMyUjRWc09FVVU9fJgmguEcQE9NGiD_vYv4ymZpnsmOBH4btJX1l56WY4V7',
                        'X-API-Token': 'S73P423fuI4LONnZSyhz42MnEYM5UapA'
                        }
        self.result = []

    def get_base_category(self):
        base_url = self.base_url.format('')
        ret = requests.get(base_url, headers=self.headers)
        payload = ret.json()['payload']
        res = payload.get('children', [])
        for row in res:
            cate = row['id']
            yield cate

    def get_leave_category(self, cate_id, parent):
        base_url = self.base_url.format(cate_id)
        ret = requests.get(base_url, headers=self.headers)
        payload = ret.json()['payload']
        res = payload.get('children', [])
        for row in res:
            cate = row['id']
            flag = row['hasPublicChildren']
            name = row['name']
            if flag:
                self.get_leave_category(cate, parent)
            else:
                print(f'get {cate}')
                ele = {'cateId': cate, 'cateName': name, 'parentId': parent}
                self.result.append(ele)

    def save(self):
        sql = 'insert ignore into proCenter.joom_category(cateName, cateId,cateLevel, parentCateId) values(%s, %s,%s, %s)'
        for row in self.result:
            self.warehouse_cur.execute(sql, (row['cateName'], row['cateId'], 2, row['parentId']))
            print(f'putting {row["cateName"]}')
            self.warehouse_con.commit()

    def run(self):
        try:
            ret = self.get_base_category()
            for cate in ret:
                self.get_leave_category(cate, cate)
            self.save()

        except Exception as why:
            self.logger.error(f'failed to fetch joom category cause of {why}')

        finally:
            self.close()


if __name__ == '__main__':
    worker = JoomCategoryFetcher()
    worker.run()
