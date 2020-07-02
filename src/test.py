#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-20 15:56
# Author: turpure

import click
import copy
import json
import requests
import hmac
from hashlib import sha1
from src.services.base_service import BaseService
from src.services import oauth as aliOauth


class AliSync(BaseService):
    """
    check purchased orders
    """

    def __init__(self, id):
        super().__init__()
        self.id = id


    def get_request_url(self, oauth, params):
        signature = self.get_signature(oauth, params)
        head = [
            f"http://gw.open.1688.com:80/openapi/param2/1/{params['api_type']}",
            params['api_name'],
            oauth.app_key
        ]
        url_head = '/'.join(head)
        para_dict = params
        para_dict['_aop_signature'] = signature
        del para_dict['api_type']
        del para_dict['api_name']

        parameter = [key + "=" + para_dict[key] for key in para_dict]
        url_tail = "&".join(parameter)
        base_url = url_head + "?" + url_tail
        return base_url



    def get_signature(self, oauth, params):
        url_path = 'param2/1/%s/%s/%s' % (params['api_type'], params['api_name'], oauth.app_key)

        signature_par_dict = copy.deepcopy(params)
        del signature_par_dict['api_type']
        del signature_par_dict['api_name']


        ordered_par_dict = sorted(key + signature_par_dict[key] for key in signature_par_dict)
        par_string = ''.join(ordered_par_dict)
        raw_string = url_path + par_string
        signature = hmac.new(bytes(oauth.app_secret_key, 'utf-8'),
                             bytes(raw_string, 'utf-8'),
                             sha1).hexdigest().upper()
        return signature


    # @retry(stop=stop_after_attempt(1))
    def get_1688_goods_details(self, urlInfo):
        goodsUrl = urlInfo['LinkUrl']
        try:
            goodsId = goodsUrl.split('/')[-1].split('.')[0]
        except:
            goodsId = ''
        urlInfo['1688GoodsId'] = goodsId
        infoId = urlInfo['ID']
        oauth = aliOauth.Ali(urlInfo['AliasName'])
        params = {
            'webSite': '1688',
            'productID': goodsId,
            'access_token': oauth.token,
            'api_type': 'com.alibaba.product',
            'api_name': 'alibaba.agent.product.simple.get'
        }
        base_url = self.get_request_url(oauth, params)
        print(base_url)

        out = dict()
        try:
            res = requests.get(base_url)
            ret = json.loads(res.content)
            out = dict()
            if 'productInfo' in ret:
                for sku in ret['productInfo']['skuInfos']:
                    out['infoId'] = infoId
                    out['offerId'] = goodsId
                    out['specId'] = sku['specId']
                    out['subject'] = ret['productInfo']['subject']
                    out['style'] = ''
                    out['multiStyle'] = 0
                    out['supplierLoginId'] = ret['productInfo']['sellerLoginId']
                    out['companyName'] = ret['productInfo']['sellerLoginId']
                    for attr in sku['attributes']:
                        if attr['attributeDisplayName'] == '颜色':
                            out['style'] = attr['attributeValue']
                            break

                    row = (out['infoId'],out['offerId'],out['specId'],out['subject'],out['style'],out['multiStyle'],out['supplierLoginId'],out['companyName'])

                    self.insert(row)

        except Exception as e:
            self.logger.error('error while get order details %s' % e)
            return out


    def get_goods_url(self):
        query = ("SELECT * FROM ( "
                 "select vendor1 AS LinkUrl,ID,goodsCode from proCenter.oa_goodsinfo gs "
                 "LEFT JOIN proCenter.oa_goods g ON g.nid=gs.goodsId "
                 "where LOCATE('detail.1688.com/offer',vendor1)>0  union "
                 "select vendor2 AS LinkUrl,ID,goodsCode from proCenter.oa_goodsinfo gs "
                 "LEFT JOIN proCenter.oa_goods g ON g.nid=gs.goodsId "
                 "where LOCATE('detail.1688.com/offer',vendor2)>0 union "
                 "select vendor3 AS LinkUrl,ID,goodsCode from proCenter.oa_goodsinfo gs "
                 "LEFT JOIN proCenter.oa_goods g ON g.nid=gs.goodsId "
                 "where LOCATE('detail.1688.com/offer',vendor3)>0 "
                 ") a WHERE id = %s")
        self.warehouse_cur.execute(query, (self.id))
        ret = self.warehouse_cur.fetchall()
        tokenInfo = self.get_token_details()
        for row in ret:
            yield dict(row, **tokenInfo)

    def get_token_details(self):
        query = (" select m.AliasName, m.LastSyncTime,m.AccessToken,m.RefreshToken  "
                 "from S_AlibabaCGInfo m with(nolock)  "
                 "inner join S_AlibabaCGInfo d with(nolock) on d.mainLoginId=m.loginId  "
                 "where d.AliasName='caigoueasy'")
        self.cur.execute(query, (self.id))
        ret = self.cur.fetchone()
        return ret

    def clear(self):
        query = (" delete from proCenter.oa_goods1688 where infoId = %s")
        self.warehouse_cur.execute(query, (self.id))
        self.warehouse_con.commit()

    def insert(self, row):
        query = (" insert into proCenter.oa_goods1688(infoId,offerId,specId,subject,style,multiStyle,supplierloginId,companyName)"
                 "values (%s,%s,%s,%s,%s,%s,%s,%s)")
        self.warehouse_cur.execute(query, row)
        self.warehouse_con.commit()



    def run(self):
        try:
            self.clear()
            goods = self.get_goods_url()
            for good in goods:
                self.get_1688_goods_details(good)
        except Exception as e:
            self.logger(e)
        finally:
            self.close()


@click.command()
@click.option('--id', default='54930', help='商品id 参数，必须，oagoodsinfo商品表ID')
def work(id):
    sync = AliSync(id=id)
    sync.run()


if __name__ == '__main__':
    work()
