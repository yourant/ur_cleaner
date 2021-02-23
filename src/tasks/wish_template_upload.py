#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-02-22 11:30
# Author: turpure


import os
import datetime
from src.services.base_service import CommonService
import requests
from multiprocessing.pool import ThreadPool as Pool
from bson import ObjectId


class Worker(CommonService):
    """
    worker template
    """

    def __init__(self):
        super().__init__()
        self.base_name = 'mssql'
        self.today = datetime.datetime.today() - datetime.timedelta(hours=8)
        self.log_type = {1: "刊登商品", 2: "添加多属性"}
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.tokens = self.get_tokens()
        self.col_task = self.get_mongo_collection('operation', 'wish_task')
        self.col_temp = self.get_mongo_collection('operation', 'wish_template')
        self.col_log = self.get_mongo_collection('operation', 'wish_log')

    def close(self):
        self.base_dao.close_cur(self.cur)

    def get_wish_tasks(self):
        ret = self.col_task.find({'status': 'todo'})
        for row in ret:
            yield row

    def get_tokens(self):
        sql = "SELECT AccessToken as token,aliasname as suffix FROM S_WishSyncInfo(nolock) WHERE  " \
              " aliasname is not null and aliasname not in " \
              " (select DictionaryName from B_Dictionary(nolock) where CategoryID=12 and used=1 and FitCode='Wish')"

        self.cur.execute(sql)
        ret = self.cur.fetchall()
        tokens = dict()
        for ele in ret:
            tokens[ele['suffix']] = ele['token']
        return tokens

    def get_wish_template(self, template_id):
        try:
            template = self.col_temp.find_one({'_id': ObjectId(template_id)})
            try:
                template['access_token'] = self.tokens[template['selleruserid']]
            except Exception:
                raise ValueError(f'{template["selleruserid"]} is unused')

            template['localized_currency_code'] = template['local_currency']
            template['localized_price'] = template['local_price']
            template['localized_shipping'] = template['local_shippingfee']
            del template['_id']
            del template['creator']
            del template['created']
            del template['updated']
            del template['local_currency']
            del template['local_price']
            del template['local_shippingfee']
            template = self.get_local_info(template)
            return template
        except Exception as e:
            self.logger.error(e)
            return {}

    @staticmethod
    def get_local_info(template):
        currency_code = template['localized_currency_code']
        if currency_code == 'USD':
            template['localized_price'] = template['price']
            template['localized_shipping'] = template['shipping']

            variants = template['variants']
            for row in variants:
                row['localized_currency_code'] = currency_code
                row['localized_price'] = row['price']
                row['localized_shipping'] = row['shipping']
        return template

    def pre_check(self, template):
        try:
            tags = template['tags']
            if not tags:
                return False

            tags = str.split(tags, ',')
            variations = template['variants']
            # 检查 tags个数
            if len(tags) > 10:
                return False

            # 单属性不用验证
            if str.split(template['sku'], '@#')[0][-2::] == '01' and not variations:
                return True
            for vn in variations:

                # 颜色是否包含中文
                if self.is_contain_chinese(vn['color']):
                    return False

                # 尺寸是否包含中文
                if self.is_contain_chinese(vn['size']):
                    return False

                # 颜色和尺寸同时为空
                if (not vn['color']) and (not vn['size']):
                    return False

            return True
        except:
            return False

    @staticmethod
    def is_contain_chinese(check_str):
        """
        判断字符串中是否包含中文
        :param check_str: {str} 需要检测的字符串
        :return: {bool} 包含返回True， 不包含返回False
        """
        if not check_str:
            return False
        for ch in check_str:
            if u'\u4e00' <= ch <= u'\u9fff':
                return True
        return False

    def check_wish_template(self, row):
        url = "https://merchant.wish.com/api/v2/product"
        params = {'access_token': row['access_token'], 'parent_sku': row['sku']}
        try:
            response = requests.get(url, params=params)
            ret = response.json()
            if ret['code'] == 0:
                return ret['data']['Product']['id']
            return False
        except Exception as why:
            self.logger.error(why)
            return False

    def upload_template(self, row):

        try:
            params = {}
            task_id = row['_id']
            params['task_id'] = str(task_id)
            params['template_id'] = str(row['template_id'])
            params['selleruserid'] = row['selleruserid']
            params['sku'] = ''
            params['type'] = self.log_type[1]

            task_params = {'id': task_id, 'status': 'success'}

            # 获取模板和token信息
            template = self.get_wish_template(row['template_id'])

            # 检验模板是否有问题
            flag = self.pre_check(template)
            if not flag:
                # 标记为刊登失败
                task_params['item_id'] = ''
                task_params['status'] = 'failed'
                self.update_task_status(task_params)
                message = f"template of {row['template_id']} is invalid"
                params['info'] = message
                self.add_log(params)
                self.logger.error(message)
                return
            if template:
                parent_sku = template['sku']
                params['sku'] = parent_sku
                # 判断是否有该产品
                check = self.check_wish_template(template)
                if not check:
                    try:
                        url = 'https://merchant.wish.com/api/v2/product/add'
                        response = requests.post(url, data=template)
                        ret = response.json()
                        if ret['code'] == 0:
                            task_params['item_id'] = ret['data']['Product']['id']
                            self.upload_variation(template['variants'], template['access_token'], parent_sku, params)
                            self.update_task_status(task_params)
                            self.update_template_status(row['template_id'], ret['data']['Product']['id'])
                        else:
                            # 如果是美金账号

                            task_params['item_id'] = ''
                            task_params['status'] = 'failed'
                            self.update_task_status(task_params)
                            params['info'] = ret['message']
                            self.add_log(params)
                            self.logger.error(f"failed to upload product {parent_sku} cause of {ret['message']}")
                    except Exception as why:
                        task_params['item_id'] = ''
                        task_params['status'] = 'failed'
                        self.update_task_status(task_params)
                        self.logger.error(f"fail to upload of product {parent_sku}  cause of {why}")
                else:
                    task_params['item_id'] = check
                    self.update_task_status(task_params)
                    self.update_template_status(row['template_id'], check)
                    params['info'] = f'products {parent_sku} already exists'
                    self.add_log(params)
                    self.logger.error(f"fail cause of products {parent_sku} already exists")
            else:
                task_params['item_id'] = ''
                task_params['status'] = 'failed'
                self.update_task_status(task_params)
                params['info'] = f"can not find template {row['template_id']} Maybe the account is not available"
                self.add_log(params)
                self.logger.error(f"fail cause of can not find template {row['template_id']}")
        except Exception as e:
            self.logger.error(f"upload {str(row['template_id'])} error cause of {e}")

    def upload_variation(self, rows, token, parent_sku, params):
        params['type'] = self.log_type[2]
        try:
            url = "https://merchant.wish.com/api/v2/variant/add"
            for row in rows:
                row['access_token'] = token
                row['parent_sku'] = parent_sku
                del row['shipping']
                response = requests.post(url, data=row)
                ret = response.json()
                if ret['code'] != 0:
                    params['info'] = ret['message']
                    params['sku'] = row['sku']
                    try:
                        del params['_id']
                    except:
                        pass
                    self.add_log(params)
                    self.logger.error(f"fail to upload of products variant {row['sku']} cause of {ret['message']}")
        except Exception as why:
            params['info'] = why
            self.add_log(params)
            self.logger.error(f"fail to upload of products variants {parent_sku}  cause of {why}")

    def update_task_status(self, row):
        self.col_task.update_one({'_id': row['id']}, {"$set": {'item_id': row['item_id'], 'status': row['status'],
                                                          'updated': self.today}}, upsert=True)

    def update_template_status(self, template_id, item_id):
        self.col_temp.update_one({'_id': ObjectId(template_id)}, {"$set": {'item_id': item_id, 'status': '刊登成功',
                                                                      'is_online': 1, 'updated': self.today}}, upsert=True)

    # 添加日志
    def add_log(self, params):
        params['created'] = self.today
        self.col_log.insert_one(params)

    def work(self):
        try:
            tasks = self.get_wish_tasks()
            pl = Pool(8)
            pl.map(self.upload_template, tasks)
            pl.close()
            pl.join()


            # self.sync_data()
        except Exception as why:
            self.logger.error('fail to upload wish template cause of {} '.format(why))
            name = os.path.basename(__file__).split(".")[0]
            raise Exception(f'fail to finish task of {name}')
        finally:
            self.close()

    def sync_data(self):
        """
        同步模板和任务的状态
        :return:
        """
        tp = self.col_temp.find()
        for ele in tp:
            ret = self.col_task.find_one({'template_id': str(ele['_id']), "item_id": {'$nin': ['']}})
            item_id = ''
            if ret:
                item_id = ret.get('item_id', '')
            self.col_temp.update_one({'_id': ele['_id']}, {"$set": {'item_id': item_id, 'is_online': 1, 'status': '刊登成功'}})
            self.logger.info(f'updating template of {ele["_id"]} set item_id to {item_id}')


if __name__ == "__main__":
    worker = Worker()
    worker.work()
