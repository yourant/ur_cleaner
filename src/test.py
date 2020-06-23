#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-20 15:56
# Author: turpure

import click
import json
import requests
import datetime
import hmac
from hashlib import sha1
from tenacity import retry, stop_after_attempt
from src.services.base_service import BaseService
from src.services import oauth as aliOauth
from concurrent.futures import ThreadPoolExecutor


class AliSync(BaseService):
    """
    check purchased orders
    """
    def __init__(self, id):
        super().__init__()
        self.id = id

    @retry(stop=stop_after_attempt(3))
    def get_1688_goods_details(self, urlInfo):
        print(urlInfo)
        goodsUrl = urlInfo['LinkUrl']
        try:
            goodsId = goodsUrl.split('/')[-1].split('.')[0]
        except:
            goodsId = ''
        urlInfo['1688GoodsId'] = goodsId
        infoId = urlInfo['NID']
        oauth = aliOauth.Ali(urlInfo['AliasName'])
        base_url = self.get_request_url(goodsId, oauth)
        print(base_url)
        out = dict()
        # try:
        #     res = requests.get(base_url)
        #     ret = json.loads(res.content)['result']
        #     out['order_id'] = order_id
        #     out['expressFee'] = float(ret['baseInfo']['shippingFee'])
        #     out['sumPayment'] = float(ret['baseInfo']['totalAmount'])
        #     out['qty'] = sum([ele['quantity'] for ele in ret['productItems']])
        #     return out
        # except Exception as e:
        #     self.logger.error('error while get order details %s' % e)
        #     return out

    def get_request_url(self, product_id, oauth):
        token = oauth.token
        signature = self.get_signature(product_id, oauth)
        head = [
            'http://gw.open.1688.com:80/openapi/param2/1/com.alibaba.trade',
            oauth.api_name,
            oauth.app_key
        ]
        url_head = '/'.join(head)
        para_dict = {
            'webSite': '1688',
            'productID': product_id,
            '_aop_signature': signature,
            'access_token': token
        }
        parameter = [key + "=" + para_dict[key] for key in para_dict]
        url_tail = "&".join(parameter)
        base_url = url_head + "?" + url_tail
        return base_url


    def get_signature(self, product_id, oauth):
        url_path = 'param2/1/com.alibaba.trade/%s/%s' % (oauth.api_name, oauth.app_key)
        token = oauth.token
        signature_par_dict = {
            'webSite': '1688',
            'access_token': token,
            'productID': product_id
        }
        ordered_par_dict = sorted(key + signature_par_dict[key] for key in signature_par_dict)
        par_string = ''.join(ordered_par_dict)
        raw_string = url_path + par_string
        signature = hmac.new(bytes(oauth.app_secret_key, 'utf-8'),
                             bytes(raw_string, 'utf-8'),
                             sha1).hexdigest().upper()
        return signature



    def check_order(self, check_info):
        order_id = check_info['order_id']
        search_sql = ("select cgsm.billnumber," 
                    "sum(sd.amount) total_amt," 
                    "sum(sd.amount*sd.price) as total_money, "
                    "sum(sd.amount * gs.costprice) as total_cost_money "   # 2020-06-22  修改
                    "from cg_stockorderd  as sd "
                    "LEFT JOIN cg_stockorderm  as cgsm on sd.stockordernid= cgsm.nid " 
                    "LEFT JOIN b_goodssku  as gs on sd.goodsskuid= gs.nid " 
                    "where alibabaOrderid = %s " 
                    "GROUP BY cgsm.billnumber, cgsm.nid,cgsm.recorder," 
                    "cgsm.expressfee,cgsm.audier,cgsm.audiedate,cgsm.checkflag ")

        check_sql = "P_CG_UpdateStockOutOfByStockOrder %s"

        # update_status = "update cg_stockorderM  set ordermoney=%s where billNumber = %s"

        update_sql = ("update cg_stockorderM  set alibabaorderid=%s," 
                     # "expressFee=%s-%s, alibabamoney=%s " 
                     "expressFee=%s, alibabamoney=%s, ordermoney=%s" 
                     "where billNumber = %s")

        update_price = "update cgd set money= gs.costprice * amount + amount*(%s-%s)/%s," \
                       "allmoney= gs.costprice * amount + amount*(%s-%s)/%s, " \
                       "cgd.beforeavgprice= gs.costprice, " \
                       "cgd.price= gs.costprice ," \
                       "cgd.taxprice= gs.costprice + (%s-%s)/%s " \
                       "from cg_stockorderd  as cgd " \
                       "LEFT JOIN B_goodsSku as gs on cgd.goodsskuid = gs.nid " \
                       "LEFT JOIN cg_stockorderm as cgm on cgd.stockordernid= cgm.nid " \
                       "where billnumber=%s"


        try:
            self.cur.execute(search_sql,order_id)
            ret = self.cur.fetchone()
            if ret:
                qty = ret['total_amt']
                total_money = ret['total_money']
                total_cost_money = ret['total_cost_money']
                bill_number = ret['billnumber']
                check_qty = check_info['qty']
                order_money = check_info['sumPayment']
                expressFee = check_info['expressFee']
                if qty == check_qty:
                    self.cur.execute(update_sql, (order_id, expressFee, order_money, order_money, bill_number))
                    self.cur.execute(check_sql, (bill_number,))
                    self.cur.execute(update_price, (order_money, total_money, qty) * 2 + (order_money, total_cost_money, qty) * 1 + (bill_number,))
                    # self.cur.execute(update_status, (order_money, bill_number))
                    self.con.commit()
                    self.logger.info('checking %s' % bill_number)
        except Exception as e:
            self.logger.error('%s while checking %s' % (e, order_id))



    def get_goods_url(self):
        query = "EXEC B_get1688Urls %s"
        self.cur.execute(query, (self.id))
        ret = self.cur.fetchall()
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


    def check(self, urlInfo):
        try:

            ret = self.get_1688_goods_details(urlInfo)
            # if ret:
            #     self.check_order(ret)
        except Exception as e:
            self.logger.error(e)

    def run(self):
        try:
            goods = self.get_goods_url()
            for good in goods:
                self.check(good)
        except Exception as e:
            self.logger(e)
        finally:
            self.close()


@click.command()
@click.option('--id',default='54807',help='商品id 参数，必须，oagoodsinfo商品表ID')
def work(id):
    sync = AliSync(id=id)
    sync.run()


if __name__ == '__main__':
    work()






