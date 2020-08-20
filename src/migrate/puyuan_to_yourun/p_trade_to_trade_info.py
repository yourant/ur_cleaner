#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-08-19 17:21
# Author: turpure


from src.services.base_service import BaseService


class Worker(BaseService):
    """
    p_trade 表数据迁移到trade_info里面
    """

    def get_data_from_old_base(self, plat):

        sql = f"select top 100 * from p_trade_his(nolock) where addressowner='{plat}' order by nid desc  "
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield (

                row['NID'],
                row['RECEIVERBUSINESS'],
                row['RECEIVEREMAIL'],
                row['RECEIVERID'],
                row['EMAIL'],
                row['PAYERID'],
                row['PAYERSTATUS'],
                row['COUNTRYCODE'],
                row['PAYERBUSINESS'],
                row['SALUTATION'],
                row['FIRSTNAME'],
                row['MIDDLENAME'],
                row['LASTNAME'],
                row['SUFFIX'],
                row['ADDRESSOWNER'],
                row['ADDRESSSTATUS'],
                row['SHIPTONAME'],
                row['SHIPTOSTREET'],
                row['SHIPTOSTREET2'],
                row['SHIPTOCITY'],
                row['SHIPTOSTATE'],
                row['SHIPTOZIP'],
                row['SHIPTOCOUNTRYCODE'],
                row['SHIPTOCOUNTRYNAME'],
                row['SHIPTOPHONENUM'],
                row['TRANSACTIONID'],
                row['PARENTTRANSACTIONID'],
                row['RECEIPTID'],
                row['TRANSACTIONTYPE'],
                row['PAYMENTTYPE'],
                row['ORDERTIME'],
                row['AMT'],
                row['CURRENCYCODE'],
                row['FEEAMT'],
                row['SETTLEAMT'],
                row['TAXAMT'],
                row['EXCHANGERATE'],
                row['PAYMENTSTATUS'],
                row['PENDINGREASON'],
                row['REASONCODE'],
                row['PROTECTIONELIGIBILITY'],
                row['PROTECTIONELIGIBILITYTYPE'],
                row['INVNUM'],
                row['CUSTOM'],
                row['NOTE'],
                row['SALESTAX'],
                row['BUYERID'],
                None if row['CLOSINGDATE'] == '' else row['CLOSINGDATE'],
                row['MULTIITEM'],
                row['TIMESTAMP'],
                row['SHIPDISCOUNT'],
                row['INSURANCEAMOUNT'],
                row['CORRELATIONID'],
                row['ACK'],
                row['VERSION'],
                row['BUILD'],
                row['SHIPPINGAMT'],
                row['HANDLINGAMT'],
                row['SHIPPINGMETHOD'],
                row['SHIPAMOUNT'],
                row['SHIPHANDLEAMOUNT'],
                row['SUBJECT'],
                row['EXPECTEDECHECKCLEARDATE'],
                row['Guid'],
                row['BUSINESS'],
                row['User'],
                row['TotalWeight'],
                row['ExpressNID'],
                row['ExpressFare'],
                row['logicsWayNID'],
                row['SelFlag'],
                row['TrackNo'],
                row['ExpressFare_Close'],
                row['ExpressStatus'],
                row['EvaluateStatus'],
                row['TransMail'],
                row['FilterFlag'],
                row['PrintFlag'],
                row['ShippingStatus'],
                row.get('MergeFlag', row.get('[MergeBillID', 0)),
                row['Memo'],
                row['AdditionalCharge'],
                row['InsuranceFee'],
                row['AllGoodsDetail'],
                row['GoodItemIDs'],
                row['CheckOrder'],
                row['IsPackage'],
                row['IsChecked'],
                row['IsPacking'],
                row['RestoreStock'],
                row['BatchNum'],
                row['colorFlag'],
                row['PackingMen'],
                row['PackageMen'],
                row['PaidanDate'],
                row['PaidanMen'],
                row['ScanningMen'],
                row['WeighingMen'],
                row['ScanningDate'],
                row['WeighingDate'],
                row['OrigPackingMen'],
                row['OrigPackageMen'],
                row['GoodsCosts'],
                row['ProfitMoney'],
                row['doorplate']
            )

    def put_data_to_new_base_single(self, rows):

        sql = 'insert into trade_info(nid, receiver_business, receiver_email, receiver_id, email, payer_id, payer_status, country_code, payer_business, salutation, first_name, middle_name, last_name, suffix, address_owner, address_status, ship_to_name, ship_to_street, ship_to_street2, ship_to_city, ship_to_state, ship_to_zip, ship_to_country_code, ship_to_country_name, ship_to_phone_number, transaction_id, parent_transaction_id, receipt_id, transaction_type, payment_type, ordertime, amt, currecny_code, fee_amt, settle_amt, tax_amt, exchange_rate, payment_status, pending_reason, reason_code, protection_qualification, trade_exception_type, inv_number, custom, note, sales_tax, buer_id, closingdate, mult_item, time_stamp, ship_discount, insurance_amount, correlation_id, ack, version, build, shipping_amt, handling_amt, shipping_method, ship_amount, ship_handle_amount, subject, expected_check_clear_date, guid, bussiness, user, total_weight, express_nid, express_fare, logics_way_nid, sel_flag, track_number, express_fare_close, express_status, evaluate_status, trans_mail, trade_flag, print_flag, shipping_status, merge_flag, memo, additional_charge, insurance_fee, all_goods_detail, good_item_ids, check_order, is_package, is_checked, is_packing, restore_stock, batch_number, colorFlag, packing_men, package_men, paidan_date, paidan_men, scanning_men, weighing_men, scanning_date, weighing_date, orig_packing_men, orig_package_men, goods_cost, profit_money, doorplate) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        for rw in rows:
            try:
                self.erp_cur.execute(sql, rw)
                self.erp_con.commit()
                self.logger.info(f'success to save data')
            except Exception as why:
                self.erp_con.rollback()
                self.logger.error(f'fail to save data cause of {why} of {rw[0]}')

    def put_data_to_new_base_multiple(self, rows):

        sql = 'insert into trade_info(nid, receiver_business, receiver_email, receiver_id, email, payer_id, payer_status, country_code, payer_business, salutation, first_name, middle_name, last_name, suffix, address_owner, address_status, ship_to_name, ship_to_street, ship_to_street2, ship_to_city, ship_to_state, ship_to_zip, ship_to_country_code, ship_to_country_name, ship_to_phone_number, transaction_id, parent_transaction_id, receipt_id, transaction_type, payment_type, ordertime, amt, currecny_code, fee_amt, settle_amt, tax_amt, exchange_rate, payment_status, pending_reason, reason_code, protection_qualification, trade_exception_type, inv_number, custom, note, sales_tax, buer_id, closingdate, mult_item, time_stamp, ship_discount, insurance_amount, correlation_id, ack, version, build, shipping_amt, handling_amt, shipping_method, ship_amount, ship_handle_amount, subject, expected_check_clear_date, guid, bussiness, user, total_weight, express_nid, express_fare, logics_way_nid, sel_flag, track_number, express_fare_close, express_status, evaluate_status, trans_mail, trade_flag, print_flag, shipping_status, merge_flag, memo, additional_charge, insurance_fee, all_goods_detail, good_item_ids, check_order, is_package, is_checked, is_packing, restore_stock, batch_number, colorFlag, packing_men, package_men, paidan_date, paidan_men, scanning_men, weighing_men, scanning_date, weighing_date, orig_packing_men, orig_package_men, goods_cost, profit_money, doorplate) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        try:
            self.erp_cur.executemany(sql, rows)
            self.erp_con.commit()
            self.logger.info(f'success to save data')
        except Exception as why:
            self.erp_con.rollback()
            self.logger.error(f'fail to save data cause of {why}')

    def run(self):
        try:
            plat = ['ebay', 'wish', 'joom', 'vova', 'aliexpress', 'amazon11']
            for pt in plat:
                rows = self.get_data_from_old_base(pt)
                self.put_data_to_new_base_multiple(rows)

        except Exception as why:
            self.logger.error(f'fail to get data cause of {why}')

        finally:
            self.close()


if __name__ == '__main__':
    worker = Worker()
    worker.run()
