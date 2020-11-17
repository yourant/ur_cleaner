#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-10-09 11:30
# Author: Henry


import os
import datetime
from src.services.base_service import CommonService
import requests
from multiprocessing.pool import ThreadPool as Pool
from bson import ObjectId
from pymongo import MongoClient
from ebaysdk.trading import Connection as Trading
from ebaysdk import exception
from configs.config import Config

mongo = MongoClient('192.168.0.150', 27017)
mongodb = mongo['operation']
col_temp = mongodb['ebay_template']
col_task = mongodb['ebay_task']
col_log = mongodb['ebay_log']


class Worker(CommonService):
    """
    worker template
    """

    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')
        self.base_name = 'mssql'
        self.today = datetime.datetime.today() - datetime.timedelta(hours=8)
        self.log_type = {1: "刊登商品", 2: "添加多属性"}
        self.cur = self.base_dao.get_cur(self.base_name)
        self.con = self.base_dao.get_connection(self.base_name)
        self.tokens = self.get_tokens()

    def close(self):
        self.base_dao.close_cur(self.cur)

    @staticmethod
    def get_ebay_tasks():
        ret = col_task.find({'status': 'todo'})
        for row in ret:
            yield row

    def get_tokens(self):
        sql = ("SELECT MAX(EbayToken) as token,noteName as suffix, EuSellerID as sellerID FROM S_PalSyncInfo(nolock) " +
               "WHERE noteName is not null and noteName not in " +
               "(select DictionaryName from B_Dictionary(nolock) where CategoryID=12 and used=1 and FitCode='eBay') " +
               "GROUP BY noteName,EuSellerID")

        self.cur.execute(sql)
        ret = self.cur.fetchall()
        tokens = dict()
        for ele in ret:
            tokens[ele['suffix']] = ele['token']
            tokens[ele['sellerID']] = ele['token']
        return tokens

    def get_ebay_template(self, template_id):
        try:
            template = col_temp.find_one({'_id': ObjectId(template_id)})
            try:
                token = self.tokens[template['SiteInfo']['Suffix']]
            except Exception:
                raise ValueError(f'template {template_id} is not found')
            # print(token)
            site = int(template['SiteInfo']['Site'])
            if site == 77 or site == 3:
                upc = 'EAN'
            else:
                upc = 'UPC'
            data = {
                'requesterCredentials': {'eBayAuthToken': token},
                "Item": {
                    # 拍卖立即购买价格
                    # "BuyItNowPrice": float(template['GoodsInfo']['BuyItNowPrice']),
                    # 自动映射新的分类ID到分类（所选分类失效的情况下），默认true
                    # "CategoryMappingAllowed": " boolean ",

                    "ConditionID": str(int(template['SiteInfo']['ConditionID'])),
                    "Country": template['LocationInfo']['Country'],
                    # "CrossBorderTrade": " string ",
                    "Currency": template['GoodsInfo']['Currency'],
                    "Description": "<![CDATA[" + template['Description'] + "]]>",
                    "DispatchTimeMax": int(template['DispatchTimeMax']),
                    "HitCounter": "NoHitCounter",
                    # 物品属性键值对
                    "ItemSpecifics": template['SiteInfo']['ItemSpecifics'],
                    #
                    "ListingDuration": template['GoodsInfo']['ListingDuration'],
                    # 销售方式
                    "ListingType": template['SiteInfo']['ListingType'],
                    "Location": template['LocationInfo']['Location'],
                    "PaymentMethods": template['PaymentInfo']['PaymentMethods'],
                    "PayPalEmailAddress": template['PaymentInfo']['PayPalEmailAddress'],
                    "PictureDetails": {
                        "GalleryType": template['PictureInfo']['GalleryType'],
                        # "PhotoDisplay": template['PictureInfo']['PayPalEmailAddress'],
                        "PictureURL": template['PictureInfo']['PictureURL'][:12]
                    },
                    "PostalCode": template['LocationInfo']['PostalCode'],
                    "PrimaryCategory": {
                        "CategoryID": str(int(template['SiteInfo']['FirstCategory']))
                    },

                    # 剩余物品最小值
                    # "QuantityInfo": {
                    #     "MinimumRemnantSet": " int "
                    # },
                    # 用户购买最大值
                    # "QuantityRestrictionPerBuyer": {
                    #     "MaximumQuantity": " int "
                    # },
                    # "ReservePrice": {
                    #     "-currencyID": "CurrencyCodeType",
                    #     "#text": " AmountType (double) "
                    # },
                    # 退货政策
                    "ReturnPolicy": {
                        "Description": template['ReturnPolicyInfo']['Description'],
                        "InternationalRefundOption": template['ReturnPolicyInfo']['InternationalRefundOption'],
                        "InternationalReturnsAcceptedOption": template['ReturnPolicyInfo'][
                            'InternationalReturnsAcceptedOption'],
                        "InternationalReturnsWithinOption": template['ReturnPolicyInfo'][
                            'InternationalReturnsWithinOption'],
                        "InternationalShippingCostPaidByOption": template['ReturnPolicyInfo'][
                            'InternationalShippingCostPaidByOption'],
                        "RefundOption": template['ReturnPolicyInfo']['RefundOption'],
                        "ReturnsAcceptedOption": template['ReturnPolicyInfo']['ReturnsAcceptedOption'],
                        "ReturnsWithinOption": template['ReturnPolicyInfo']['ReturnsWithinOption'],
                        "ShippingCostPaidByOption": template['ReturnPolicyInfo']['ShippingCostPaidByOption'],
                    },
                    # 物流信息
                    "ShippingDetails": {
                        # "ExcludeShipToLocation": template['ShippingInfo']['ExcludeShipToLocation'],
                        "InternationalShippingServiceOption": {},
                        "ShippingServiceOptions": {},
                    },
                    # "ShipToLocations": " string ",
                    # "Site": template['SiteInfo']['Site'],
                    "SiteId": site,
                    "SKU": template['GoodsInfo']['SKU'],
                    # "StartPrice": float(template['GoodsInfo']['BuyItNowPrice']),
                    "SubTitle": template['GoodsInfo']['SubTitle'],
                    "Title": template['GoodsInfo']['Title'],
                    "VATDetails": {
                        "VATPercent": int(template['GoodsInfo']['VatTax']) if template['GoodsInfo']['VatTax'] else 0,
                    },
                    # "VIN": " string ",
                    # "VRM": " string "
                },
            }
            # print(type(template['SiteInfo']['SecondCategory']))
            # print(str(int(template['SiteInfo']['ConditionID'])))
            if template['GoodsInfo']['LotSize'] and int(template['GoodsInfo']['LotSize']) > 1:
                data['Item']["LotSize"] = int(template['GoodsInfo']['LotSize']),
            # 第二刊登分类
            if template['SiteInfo']['SecondCategory']:
                data['Item']["SecondaryCategory"]["CategoryID"] = template['SiteInfo']['SecondCategory']
            # 买家限制
            buyer_requirement = template['BuyerRequirementInfo']
            if buyer_requirement:
                data['Item']['BuyerRequirementDetails'] = {
                    "MaximumItemRequirements": {
                        "MaximumItemCount": buyer_requirement['MaximumItemRequirements']['MaximumItemCount'],
                        "MinimumFeedbackScore": buyer_requirement['MaximumItemRequirements']['MinimumFeedbackScore']
                    },
                    "MaximumUnpaidItemStrikesInfo": {
                        "Count": buyer_requirement['MaximumUnpaidItemStrikesInfo']['Count'],
                        "Period": buyer_requirement['MaximumUnpaidItemStrikesInfo']['Period']
                    },
                    "ShipToRegistrationCountry": buyer_requirement['ShipToRegistrationCountry'],
                    "ZeroFeedbackScore": buyer_requirement['ZeroFeedbackScore']
                }
            # 物流设置
            shipping_info = []
            international_shipping_info = []
            for item in template['ShippingInfo']['ShippingServiceOptions']:
                if item['ShippingService']:
                    item['ShippingServiceCost'] = int(item['ShippingServiceCost']) if item['ShippingServiceCost'] else 0
                    item['ShippingServiceAdditionalCost'] = int(item['ShippingServiceAdditionalCost']) if item[
                        'ShippingServiceAdditionalCost'] else 0
                    shipping_info.append(item)
            for item_i in template['ShippingInfo']['InternationalShippingServiceOption']:
                if item_i['ShippingService']:
                    item_i['ShippingServiceCost'] = int(item_i['ShippingServiceCost']) if item_i[
                        'ShippingServiceCost'] else 0
                    item_i['ShippingServiceAdditionalCost'] = int(item_i['ShippingServiceAdditionalCost']) if item_i[
                        'ShippingServiceAdditionalCost'] else 0
                    international_shipping_info.append(item_i)
            data['Item']['ShippingDetails']['InternationalShippingServiceOption'] = international_shipping_info
            data['Item']['ShippingDetails']['ShippingServiceOptions'] = shipping_info
            # 多属性
            variations = {'VariationSpecificsSet': {'NameValueList': []}, 'Pictures': {}, 'Variation': []}
            if template['Variations']:
                variations['Pictures']['VariationSpecificName'] = template['Variations']['assoc_pic_key']
                variations['Variation'] = template['Variations']['Variation']
                name_value_list = []
                for k, item_var in enumerate(template['Variations']['Variation']):
                    if k == 0:
                        for var in item_var['VariationSpecifics']['NameValueList']:
                            if var["Name"] == 'UPC' or var["Name"] == 'EAN':
                                name_value_list.append({"Name": upc, "Value": [var["Value"]]})
                            else:
                                name_value_list.append({"Name": var["Name"], "Value": [var["Value"]]})
                    else:
                        for var_m in item_var['VariationSpecifics']['NameValueList']:
                            for value_list in name_value_list:
                                if var_m['Name'] == value_list['Name'] and var_m['Value'] not in value_list['Value']:
                                    value_list['Value'].append(var_m['Value'])
                variations['VariationSpecificsSet']['NameValueList'] = name_value_list
                pictures = []
                for item_pic in template['Variations']['Pictures']:
                    pictures.append({"VariationSpecificValue": item_pic["Value"],
                                     "PictureURL": item_pic["VariationSpecificPictureSet"]["PictureURL"]})
                variations['Pictures']['VariationSpecificPictureSet'] = pictures
                data['Item']['Variations'] = variations
            # 单属性
            else:
                data['Item']["Quantity"] = int(template['GoodsInfo']['Quantity'])
                data['Item']["StartPrice"] = float(template['GoodsInfo']['BuyItNowPrice'])
                data['Item']["ProductListingDetails"][upc] = float(template['SiteInfo']['UPC'])
                # data['Item']["ProductListingDetails"]['EAN'] = float(template['SiteInfo']['EAN'])
            return data

        except Exception as e:
            self.logger.error(e)
            return {}

    def pre_check(self, template):
        try:
            variations = template['Item']['Variations']
            # 单属性不用验证
            if str.split(template['Item']['SKU'], '@#')[0][-2::] == '01' and not variations:
                return True

            for vn in variations['Variation']:
                i = 0
                for v in vn['VariationSpecifics']['NameValueList']:
                    # 属性是否包含中文
                    if self.is_contain_chinese(v['Value']):
                        return False
                    # UPC不能为空
                    if v['Name'] == 'UPC' and not v['Value']:
                        return False
                    # 属性为空，计数加1
                    if not v['Value']:
                        i = i + 1
                # UPC 以外的属性不能同时为空
                if i + 1 == len(vn['VariationSpecifics']['NameValueList']):
                    return False
            return True
        except Exception as e:
            print(e)
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

    def check_ebay_template(self, api, row):
        try:
            trade_response = api.execute(
                'GetItem',
                {
                    'SKU': row['Item']['SKU'],
                    # 'SKU': '7C2796@#01',
                    'requesterCredentials': row['requesterCredentials'],
                }
            )
            ret = trade_response.dict()
            if ret['Ack'] == 'Success':
                return ret['data']['Item']['ItemID']
            return False
        except exception.ConnectionError as why:
            self.logger.error(why)
            return False

    def upload_template(self, row):
        try:
            api = Trading(config_file=self.config)
            params = {}
            task_id = row['_id']
            params['task_id'] = str(task_id)
            params['template_id'] = str(row['template_id'])
            params['suffix'] = row['suffix']
            params['sku'] = ''
            params['type'] = self.log_type[1]

            task_params = {'id': task_id, 'status': 'success'}

            # 获取模板和token信息
            template = self.get_ebay_template(row['template_id'])
            if template:
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
                parent_sku = template['Item']['SKU']
                params['sku'] = parent_sku
                # 判断ebay后台是否有该产品
                # print(template)
                check = self.check_ebay_template(api, template)
                print(check)
                if not check:
                    # try:
                    trade_response = api.execute(
                        'VerifyAddFixedPriceItem',
                        # 'AddFixedPriceItem',
                        template
                    )
                    ret = trade_response.dict()
                    print(ret)
            #             if ret['Ack'] == 'Success':
            #                 task_params['item_id'] = ret['data']['Item']['ItemID']
            #                 # self.upload_variation(template['variants'], template['access_token'], parent_sku, params)
            #                 self.update_task_status(task_params)
            #                 self.update_template_status(row['template_id'], ret['data']['Product']['id'])
            #             else:
            #                 params['info'] = ret['message']
            #                 self.add_log(params)
            #                 self.logger.error(f"failed to upload product {parent_sku} cause of {ret['message']}")
            #         except Exception as why:
            #             self.logger.error(f"fail to upload of product {parent_sku}  cause of {why}")
            #     else:
            #         task_params['item_id'] = check
            #         self.update_task_status(task_params)
            #         self.update_template_status(row['template_id'], check)
            #         params['info'] = f'products {parent_sku} already exists'
            #         self.add_log(params)
            #         self.logger.error(f"fail cause of products {parent_sku} already exists")
            # else:
            #     task_params['item_id'] = ''
            #     task_params['status'] = 'failed'
            #     self.update_task_status(task_params)
            #     params['info'] = f"can not find template {row['template_id']} Maybe the account is not available"
            #     self.add_log(params)
            #     self.logger.error(f"fail cause of can not find template {row['template_id']}")
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
        col_task.update_one({'_id': row['id']}, {"$set": {'item_id': row['item_id'], 'status': row['status'],
                                                          'updated': self.today}}, upsert=True)

    def update_template_status(self, template_id, item_id):
        col_temp.update_one({'_id': ObjectId(template_id)}, {"$set": {'item_id': item_id, 'status': '刊登成功',
                                                                      'is_online': 1, 'updated': self.today}},
                            upsert=True)

    # 添加日志
    def add_log(self, params):
        params['created'] = self.today
        col_log.insert_one(params)

    def work(self):
        try:
            tasks = self.get_ebay_tasks()
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
            mongo.close()

    def sync_data(self):
        """
        同步模板和任务的状态
        :return:
        """
        tp = col_temp.find()
        for ele in tp:
            ret = col_task.find_one({'template_id': str(ele['_id']), "item_id": {'$nin': ['']}})
            item_id = ''
            if ret:
                item_id = ret.get('item_id', '')
            col_temp.update_one({'_id': ele['_id']}, {"$set": {'item_id': item_id, 'is_online': 1, 'status': '刊登成功'}})
            self.logger.info(f'updating template of {ele["_id"]} set item_id to {item_id}')


if __name__ == "__main__":
    worker = Worker()
    worker.work()
