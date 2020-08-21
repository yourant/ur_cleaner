#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-08-21 17:21
# Author: turpure


from src.services.base_service import BaseService


class Worker(BaseService):
    """
    p_trade 表数据迁移到trade_info里面
    """

    def get_data_from_old_base(self):

        sql = f"select  * from B_LogisticWay"
        self.cur.execute(sql)
        ret = self.cur.fetchall()
        for row in ret:
            yield (
                row['NID'],
                row['code'],
                row['name'],
                row['used'],
                row['FileName'],
                row['EUB'],
                row['AutoTrackNo'],
                row['ServiceCode'],
                row['AutoOrder'],
                row['AutoNo'],
                row['AutoTrackNoRule'],
                row['DefaultExpressNID'],
                row['Discount'],
                row['SurchargeRate'],
                row['URL'],
                row['Alicode'],
                row['SendNote'],
                row['CheckTrackingNum'],
                row['WishCode'],
                row['DunHuangCode'],
                row['emailTemplate'],
                row['emailSubject'],
                row['Remark'],
                row['EubRule'],
                row['DefaultStoreNID'],
                row['MaxWeight'],
                row['Printer'],
                row['PDFSPlaceOrder'],
                row['wishshipper'],
                row['declare_max'],
                row['TrackingLog'],
                row['CollectMark'],
                row['CollectionOvertimeDays'],
                row['TransportOvertimeDays'],
                row['SignMark'],
                row['PDFSGetLabel'],
                row['RuleMatch'],
                row['SendCollectionOvertimeDays'],
                row['ReturnMark'],
                row['UnDeliveryMark'],
                row['PickingCode'],
                row['PickingMemo'],
                row['TrackNoEffectiveHour'],
                row['AabroadMark']

            )

    def put_data_to_new_base_single(self, rows):

        sql = 'insert into express_logistic_way values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
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
            rows = self.get_data_from_old_base()
            self.put_data_to_new_base_single(rows)

        except Exception as why:
            self.logger.error(f'fail to get data cause of {why}')

        finally:
            self.close()


if __name__ == '__main__':
    worker = Worker()
    worker.run()
