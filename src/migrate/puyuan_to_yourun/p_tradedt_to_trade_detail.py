#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-08-20 17:21
# Author: turpure


from src.services.base_service import BaseService


class Worker(BaseService):
    """
    p_tradedt 表数据迁移到trade_detail里面
    """

    def get_data_from_old_base(self):

        sql = f"select top 100 * from p_tradedt(nolock)  order by nid desc  "
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield (
                row['NID'],
                row['TradeNID'],
                str(row['L_EBAYITEMTXNID']),
                row['L_NAME'],
                row['L_NUMBER'],
                row['L_QTY'],
                row['L_SHIPPINGAMT'],
                row['L_HANDLINGAMT'],
                row['L_CURRENCYCODE'],
                row['L_AMT'],
                row['L_OPTIONSNAME'],
                row['L_OPTIONSVALUE'],
                row['L_TAXAMT'],
                row['SKU'],
                row['CostPrice'],
                row['AliasCnName'],
                row['AliasEnName'],
                row['Weight'],
                row['DeclaredValue'],
                row['OriginCountry'],
                row['OriginCountryCode'],
                row['BmpFileName'],
                row['GoodsName'],
                row['GoodsSKUID'],
                row['StoreID'],
                row['eBaySKU'],
                row['L_ShipFee'],
                row['L_TransFee'],
                row['L_ExpressFare'],
                row['BuyerNote']

            )

    def put_data_to_new_base_single(self, rows):

        sql = 'insert into trade_detail (trade_nid, trade_detail_nid, listing_item_transaction_id, listing_name, listing_number, listing_quantity, listing_shipping_amount, listing_handling_amount, listing_currency_code, listing_amount, listing_options_name, listing_options_value, listing_tax_amount, sku, cost_price, alias_cn_name, alias_en_name, weight, declared_value, origin_country, origin_country_code, image_adress, goods_name, goods_sku_id, store_id, ebay_sku, listing_ship_fee, listing_transaciton_fee, listing_express_fare, buyer_note) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        for rw in rows:
            try:
                self.erp_cur.execute(sql, rw)
                self.erp_con.commit()
                self.logger.info(f'success to save data')
            except Exception as why:
                self.erp_con.rollback()
                self.logger.error(f'fail to save data cause of {why} of {rw[0]}')

    def put_data_to_new_base_multiple(self, rows):

        sql = 'insert into trade_detail (trade_nid, trade_detail_nid, listing_item_transaction_id, listing_name, listing_number, listing_quantity, listing_shipping_amount, listing_handling_amount, listing_currency_code, listing_amount, listing_options_name, listing_options_value, listing_tax_amount, sku, cost_price, alias_cn_name, alias_en_name, weight, declared_value, origin_country, origin_country_code, image_adress, goods_name, goods_sku_id, store_id, ebay_sku, listing_ship_fee, listing_transaciton_fee, listing_express_fare, buyer_note) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        try:
            self.erp_cur.executemany(sql, rows)
            self.erp_con.commit()
            self.logger.info(f'success to save data')
        except Exception as why:
            self.erp_con.rollback()
            self.logger.error(f'fail to save data cause of {why}')

    def run(self):
        try:
            rows = self.get_data_from_old_base()
            self.put_data_to_new_base_single(rows)

        except Exception as why:
            self.logger.error(f'fail to get data cause of {why}')

        finally:
            self.close()


if __name__ == '__main__':
    worker = Worker()
    worker.run()
