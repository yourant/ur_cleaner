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
        self.log_type = {'product': "刊登商品", 'variants': "添加多属性"}
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
            template = self.parse_template(template)
            template = self.get_local_info(template)
            return template
        except Exception as e:
            self.logger.error(e)
            raise ValueError(e)

    @staticmethod
    def parse_template(template):
        """
        整理template的结构
        :param template:
        :return:
        """
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
        return template

    @staticmethod
    def get_local_info(template):
        """
        根据货币符号处理local信息
        :param template:
        :return:
        """
        currency_code = template['localized_currency_code']
        if currency_code == 'USD':
            template['localized_price'] = template['price']
            template['localized_shipping'] = template['shipping']

            for row in template['variants']:
                row['localized_currency_code'] = currency_code
                row['localized_price'] = row['price']
                del row['shipping']

        # 删除多余字段
        else:
            for row in template['variants']:
                del row['shipping']
        return template

    def pre_check(self, template):
        try:
            tags = template['tags']
            if not tags:
                raise ValueError(f'tags of {template["sku"]}  is invalid')
            tags = str.split(tags, ',')
            variations = template['variants']
            # 检查 tags个数
            if len(tags) > 10:
                raise ValueError(f'tags of {template["sku"]} is greater than 10')

            # 单属性不用验证
            if str.split(template['sku'], '@#')[0][-2::] == '01' and not variations:
                return
            for vn in variations:

                # 颜色是否包含中文
                if self.is_contain_chinese(vn['color']):
                    raise ValueError(f'color of {template["sku"]} is Chinese ')

                # 尺寸是否包含中文
                if self.is_contain_chinese(vn['size']):
                    raise ValueError(f'size of {template["sku"]} is Chinese ')

                # 颜色和尺寸同时为空
                if (not vn['color']) and (not vn['size']):
                    raise ValueError(f'both color and size of {template["sku"]} is empty')
        except Exception as why:
            raise ValueError(f'{template["sku"]} is invalid cause of {why}')

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

            # 获取模板和token信息
            template = self.get_wish_template(row['template_id'])

            # 检验模板是否有问题,如果有错抛出异常
            self.pre_check(template)

            # 上传模板
            result = self.do_upload_template(row, template)

            # 修改状态， 记录日志
            self.add_log(result['task_log'])
            self.update_task_status(result['task_status'])
            self.update_template_status(result['template_status'])

            # 打印日志
            self.logger.info(f'success to upload template of {template["sku"]}')

        except Exception as why:
            # 记录错误日志

            # 1.任务日志
            task_id = row['_id']
            task_log = {
                'task_id': str(task_id), 'template_id': str(row['template_id']), 'selleruserid': row['selleruserid'],
                'sku': row['sku'], 'type': self.log_type['product'], 'info': ''
            }
            task_status = {'id': task_id, 'item_id': '', 'status': ''}
            self.logger.error(f"upload {str(row['template_id'])} error cause of {why}")
            task_log['info'] = f'failed to upload template because of {why}'
            self.add_log(task_log)

            # 2.任务状态
            task_status['status'] = 'failed'
            self.update_task_status(task_status)

            # 3.模板状态
            template_status = {'template_id': row['template_id'], 'item_id': '', 'status': '刊登失败', 'is_online': 0}
            self.update_template_status(template_status)

    def do_upload_template(self, task, template):
        existed = self.check_wish_template(template)

        # 1.任务日志
        task_id = task['_id']
        task_log = {
            'task_id': str(task_id), 'template_id': str(task['template_id']), 'selleruserid': task['selleruserid'],
            'sku': task['sku'], 'type': self.log_type['product'], 'info':''
        }

        # 2. 模板状态
        template_status = {'template_id': task['template_id'], 'item_id': '', 'status': '待刊登', 'is_online': 0}

        # 3. 任务状态
        task_status = {'id': task_id, 'item_id': '', 'status': ''}
        result = {'task_log': task_log, 'task_status': task_status, 'template_status': template_status}
        url = 'https://merchant.wish.com/api/v2/product/add'
        try:
            if not existed:
                res = requests.post(url, data=template)
                ret = res.json()
                if ret['code'] == 0:

                    # 添加多属性
                    self.upload_variation(template['variants'], template['access_token'], template['sku'], task_log)

                    # 模板状态
                    template_status['item_id'] = ret['data']['Product']['id']
                    template_status['status'] = '刊登成功'
                    template_status['is_online'] = 1

                    # 任务状态
                    task_status['item_id'] = ret['data']['Product']['id']
                    task_status['status'] = 'success'

                    #日志记录
                    task_log['info'] = 'success'

                else:
                    # 把错误原因写到日志

                    # 模板状态
                    template_status['status'] = '刊登失败'
                    template_status['is_online'] = 0

                    # 任务状态
                    task_status['status'] = 'failed'

                    # 日志记录
                    task_log['info'] = ret['message']
                    self.logger.error(f"failed to upload product {template['sku']} cause of {ret['message']}")
            else:
                # 产品已存在，尝试添加多属性
                self.upload_variation(template['variants'], template['access_token'], template['sku'], task_log)

                # 模板状态
                template_status['item_id'] = existed
                template_status['status'] = '刊登成功'
                template_status['is_online'] = 1

                # 任务状态
                task_status['item_id'] = existed
                task_status['status'] = 'success'

                # 日志记录
                task_log['info'] = 'success'

        except Exception as why:

            # 模板状态
            template_status['status'] = '刊登失败'
            template_status['is_online'] = 0

            # 任务状态
            task_status['status'] = 'failed'

            # 日志记录
            task_log['info'] = f"failed to upload product {template['sku']} cause of {why}"

            self.logger.error(f"failed to upload product {template['sku']} cause of {why}")

        return result

    def upload_variation(self, rows, token, parent_sku, task_log):
        task_log['type'] = self.log_type['variants']
        add_url = "https://merchant.wish.com/api/v2/variant/add"
        update_url = "https://merchant.wish.com/api/v2/variant/update"
        try:
            for row in rows:
                row['access_token'] = token
                row['parent_sku'] = parent_sku
                response = requests.post(add_url, data=row)
                ret = response.json()
                if ret['code'] != 0:

                    # do not update
                    # if 'exists' in ret['message']:
                    #  data = {'sku': row['sku'], 'main_image': row['main_image'], 'access_token': row['access_token']}
                    #     res = requests.post(update_url, data=data)

                    task_log['info'] = ret['message']
                    task_log['sku'] = row['sku']
                    try:
                        del task_log['_id']
                    except:
                        pass
                    self.add_log(task_log)
                    self.logger.error(f"failed to upload of products variant {row['sku']} cause of {ret['message']}")
        except Exception as why:
            task_log['info'] = why
            self.add_log(task_log)
            self.logger.error(f"fail to upload of products variants {parent_sku}  cause of {why}")

    def update_task_status(self, row):
        self.col_task.update_one({
            '_id': row['id']
        }, {
            "$set": {'item_id': row['item_id'], 'status': row['status'], 'updated': self.today}
        }, upsert=True)

    def update_template_status(self, template_status):
        self.col_temp.update_one({
            '_id': ObjectId(template_status['template_id'])},
            {
                "$set": {'item_id': template_status['item_id'],
                         'status': template_status['status'], 'is_online': template_status['is_online'], 'updated': self.today
                         }
            },
            upsert=True)

    # 添加日志
    def add_log(self, params):
        params['created'] = self.today
        try:
            self.col_log.insert_one(params)

        except Exception as why:
            pass

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
