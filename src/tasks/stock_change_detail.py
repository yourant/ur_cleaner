#! usr/bin/env/python3
# coding:utf-8
# @Time: 2021-01-08 13:58
# Author: turpure

from src.tasks.a_basic_task import Worker


class DataType(object):

    purchasing = 'purchasing'

    purchased = 'purchased'

    check = 'check'

    transfer = 'transfer'

    sold = 'sold'

    other = 'other'


class StockWorker(Worker):
    """
    获取每个SKU的库存变动记录
    参考： P_KC_StockTotalSummary_rjf
    """

    # 出现在配置文件的数据库名称
    def __init__(self, databases, begin_date, end_date):
        super(StockWorker, self).__init__(databases)
        self.begin_date = begin_date
        self.end_date = end_date

    def _get_sql(self, data_type, direction):

        """
        sql需要提前注册一下
        :param data_type:
        :param direction:
        :return:
        """
        if data_type == DataType.purchased:
            if direction == 'in':
                return self._get_purchased_change_in()
            if direction == 'out':
                return self._get_purchased_change_out()
        if data_type == DataType.purchasing:
            if direction == 'in':
                return self._get_purchasing_change_in()

        if data_type == DataType.check:
            if direction == 'in':
                return self._get_check_change_in()

            if direction == 'out':
                return self._get_check_change_out()

        if data_type == DataType.transfer:
            if direction == 'in':
                return self._get_transfer_change_in()

            if direction == 'out':
                return self._get_transfer_change_out()

        if data_type == DataType.sold:

            if direction == 'in':
                return self._get_sold_return_in()
            if direction == 'out':
                return self._get_sold_out()

        if data_type == DataType.other:
            return self._get_other_change_in()

        else:
            return None

    def _get_data(self, data_type, direction):

        """
        根据sql获取库存变动记录
        :param data_type:
        :return:
        """
        sql = self._get_sql(data_type, direction)
        if sql:
            cur = self.get_cur('mssql')
            cur.execute(sql)

            ret = cur.fetchall()
            if ret:
                for row in ret:
                    yield (row['sku_id'], row['sku'], row['goods_code'], row['goods_name'], row['category_name'],
                           row['category_parent_name'], row['store_name'], row['do_time'], row['do_time_type'],
                           row['do_type'], row['do_source_entry_table'], row['do_source_entry_id'],
                           row['do_source_bill_number'], row['do_amt'], row['do_quantity'])

    def data_trans(self, data_type, direction):
        """
        获取-保存数据
        :param data_type:
        :param direction:
        :return:
        """
        rows = self._get_data(data_type, direction)
        if rows:
            self._put_data(rows, data_type, direction)

    def _get_purchased_change_in(self):
        """
        采购入库明细：审核时间为准
        :return:
        """

        sql = ("SELECT    d.goodsSKuId AS sku_id, bgs.sku AS sku, bg.goodsCode AS goods_code, bg.goodsName AS goods_name,"
               "bgc.categoryname  as category_name, bgc.categoryParentName as  category_parent_name, "
               "bs.storename as store_name, m.AudieDate AS do_time, 'CG_StockInM.AudieDate' AS do_time_type, "
               "'采购入库' AS do_type, 'CG_StockInD' do_source_entry_table,d.nid AS do_source_entry_id, m.billNumber as do_source_bill_number, 	"
               "d.money AS do_amt, 	d.amount do_quantity FROM 	"
               "CG_StockInD(nolock) D INNER JOIN CG_StockInM(nolock) M ON M.NID = D.StockInNID "
               "INNER JOIN B_GoodsSKU(nolock) AS bgs ON d.goodsSKuid = bgs.nid "
               "INNER JOIN b_goods(nolock) AS bg ON bg.nid = bgs.goodsid "
               "INNER JOIN b_goodsCats(nolock) as bgc on bgc.categoryCode = bg.categoryCode "
               "INNER JOIN B_Store(nolock) as bs on bs.nid = m.storeId "
               "WHERE m.CheckFlag = 1 "
               "AND CONVERT (VARCHAR (10), 	M.AudieDate, 121 ) BETWEEN '{}'  AND '{}' AND isnull(BillType, 0) = 1")

        return sql.format(self.begin_date, self.end_date)

    def _get_other_change_in(self):

        """
        其他入库明细：审核时间为准
        :return:
        """

        sql = (
            "SELECT   d.goodsSKuId AS sku_id, bgs.sku AS sku, bg.goodsCode AS goods_code, bg.goodsName AS goods_name,"
            "bgc.categoryname  as category_name, bgc.categoryParentName as  category_parent_name, "
            "bs.storename as store_name, m.AudieDate AS do_time, 'CG_StockInM.AudieDate' AS do_time_type, "
            "'其他入库' AS do_type, 'CG_StockInD' do_source_entry_table, d.nid AS do_source_entry_id, m.billNumber as do_source_bill_number, 	"
            "d.money AS do_amt,  	d.amount do_quantity FROM 	"
            "CG_StockInD(nolock) D INNER JOIN CG_StockInM(nolock) M ON M.NID = D.StockInNID "
            "INNER JOIN B_GoodsSKU(nolock) AS bgs ON d.goodsSKuid = bgs.nid "
            "INNER JOIN b_goods(nolock) AS bg ON bg.nid = bgs.goodsid "
            "INNER JOIN b_goodsCats(nolock) as bgc on bgc.categoryCode = bg.categoryCode "
            "INNER JOIN B_Store(nolock) as bs on bs.nid = m.storeId "
            "WHERE m.CheckFlag = 1 "
            "AND CONVERT (VARCHAR (10),M.AudieDate, 121 ) BETWEEN '{}'  AND '{}' AND isnull(BillType, 0) = 3")

        return sql.format(self.begin_date, self.end_date)

    def _get_purchasing_change_in(self):
        """
        采购在途明细：审核时间为准
        :return:
        """

        sql = ("SELECT  d.goodsSKuId AS sku_id, bgs.sku AS sku, bg.goodsCode AS goods_code, bg.goodsName AS goods_name,"
               "bgc.categoryname  as category_name, bgc.categoryParentName as  category_parent_name, "
               "bs.storename as store_name, m.AudieDate AS do_time, 'CG_StockOrderM.AudieDate' AS do_time_type, "
               "'采购在途' AS do_type, 'CG_StockOrderD' do_source_entry_table,d.nid AS do_source_entry_id, m.billNumber as do_source_bill_number, 	"
               "d.money AS do_amt,  	d.amount do_quantity FROM 	"
               "CG_StockOrderD(nolock) D INNER JOIN CG_StockOrderM(nolock) M ON M.NID = D.StockOrderNid "
               "INNER JOIN B_GoodsSKU(nolock) AS bgs ON d.goodsSKuid = bgs.nid "
               "INNER JOIN b_goods(nolock) AS bg ON bg.nid = bgs.goodsid "
               "INNER JOIN b_goodsCats(nolock) as bgc on bgc.categoryCode = bg.categoryCode "
               "INNER JOIN B_Store(nolock) as bs on bs.nid = m.storeId "
               "WHERE  m.Archive = 0 AND m.checkflag = 1 AND m.inflag = 0 "
               "AND CONVERT (VARCHAR (10), 	M.AudieDate, 121 ) BETWEEN '{}' and '{}' ")

        return sql.format(self.begin_date, self.end_date)

    def _get_check_change_in(self):
        """
        盘点入库明细：审核时间为准
        :return:
        """
        sql = (
            "SELECT   d.goodsSKuId AS sku_id, bgs.sku AS sku, bg.goodsCode AS goods_code, bg.goodsName AS goods_name,"
            "bgc.categoryname  as category_name, bgc.categoryParentName as  category_parent_name, "
            "bs.storename as store_name, m.AudieDate AS do_time, 'KC_StockCheckM.AudieDate' AS do_time_type, "
            "'盘点入库' AS do_type, 'KC_StockCheckD' do_source_entry_table, d.nid AS do_source_entry_id, m.billNumber as do_source_bill_number, 	"
            "d.money AS do_amt,  	d.amount do_quantity FROM 	"
            "KC_StockCheckD(nolock) D INNER JOIN KC_StockCheckM(nolock) M ON M.NID = D.StockCheckNID "
            "INNER JOIN B_GoodsSKU(nolock) AS bgs ON d.goodsSKuid = bgs.nid "
            "INNER JOIN b_goods(nolock) AS bg ON bg.nid = bgs.goodsid "
            "INNER JOIN b_goodsCats(nolock) as bgc on bgc.categoryCode = bg.categoryCode "
            "INNER JOIN B_Store(nolock) as bs on bs.nid = m.storeId "
            "WHERE m.CheckFlag = 1 "
            "AND  (IsNull(D.Amount,0)>0 or (IsNull(D.Amount,0)=0 and IsNull(D.money,0)>0))"
            "AND CONVERT (VARCHAR (10), M.AudieDate, 121 ) BETWEEN '{}'  AND '{}'")

        return sql.format(self.begin_date, self.end_date)

    def _get_transfer_change_in(self):
        """
        调拨入库明细:审核时间为准
        :return:
        """
        sql = (
            "SELECT   d.goodsSKuId AS sku_id, bgs.sku AS sku, bg.goodsCode AS goods_code, bg.goodsName AS goods_name,"
            "bgc.categoryname  as category_name, bgc.categoryParentName as  category_parent_name, "
            "bs.storename as store_name, m.AudieDate AS do_time, 'KC_StockChangeM.AudieDate' AS do_time_type, "
            "'调拨入库' AS do_type, 'KC_StockCheckD' do_source_entry_table,   d.nid AS do_source_entry_id, m.billNumber as do_source_bill_number, 	"
            "d.money AS do_amt,  	d.amount do_quantity FROM 	"
            "KC_StockChangeD(nolock) D INNER JOIN KC_StockChangeM(nolock) M ON M.NID = D.StockChangeInNID "
            "INNER JOIN B_GoodsSKU(nolock) AS bgs ON d.goodsSKuid = bgs.nid "
            "INNER JOIN b_goods(nolock) AS bg ON bg.nid = bgs.goodsid "
            "INNER JOIN b_goodsCats(nolock) as bgc on bgc.categoryCode = bg.categoryCode "
            "INNER JOIN B_Store(nolock) as bs on bs.nid = m.storeInId "
            "WHERE m.CheckFlag = 1 "
            "AND CONVERT (VARCHAR (10), M.AudieDate, 121 ) BETWEEN '{}'  AND '{}' ")

        return sql.format(self.begin_date, self.end_date)

    def _get_sold_return_in(self):
        """
        销售退货明细：审核时间为准

        :return:
        """
        sql = (
            "SELECT d.goodsSKuId AS sku_id, bgs.sku AS sku, bg.goodsCode AS goods_code, bg.goodsName AS goods_name,"
            "bgc.categoryname  as category_name, bgc.categoryParentName as  category_parent_name, "
            "bs.storename as store_name, m.AudieDate AS do_time, 'CG_StockInM.AudieDate' AS do_time_type, "
            "'销售退货' AS do_type, 'CG_StockInD' do_source_entry_table, d.nid AS do_source_entry_id, m.billNumber as do_source_bill_number, 	"
            "d.money AS do_amt,  	d.amount do_quantity FROM 	"
            "CG_StockInD(nolock) D INNER JOIN CG_StockInM(nolock) M ON M.NID = D.StockInNID "
            "INNER JOIN B_GoodsSKU(nolock) AS bgs ON d.goodsSKuid = bgs.nid "
            "INNER JOIN b_goods(nolock) AS bg ON bg.nid = bgs.goodsid "
            "INNER JOIN b_goodsCats(nolock) as bgc on bgc.categoryCode = bg.categoryCode "
            "INNER JOIN B_Store(nolock) as bs on bs.nid = m.storeId "
            "WHERE m.CheckFlag = 1 "
            "AND CONVERT (VARCHAR (10), 	M.AudieDate, 121 ) BETWEEN '{}'  AND '{}' AND isnull(BillType, 0) = 3 "
            "and m.Memo like '退货入库%'")

        return sql.format(self.begin_date, self.end_date)

    def _get_purchased_change_out(self):
        """
        采购退货明细：审核时间为准
        :return:
        """

        sql = (
            "SELECT   d.goodsSKuId AS sku_id, bgs.sku AS sku, bg.goodsCode AS goods_code, bg.goodsName AS goods_name,"
            "bgc.categoryname  as category_name, bgc.categoryParentName as  category_parent_name, "
            "bs.storename as store_name, m.AudieDate AS do_time, 'CG_StockInM.AudieDate' AS do_time_type, "
            "'采购退货' AS do_type, 'CG_StockInD' do_source_entry_table, d.nid AS do_source_entry_id, m.billNumber as do_source_bill_number, 	"
            "d.money AS do_amt, d.amount do_quantity FROM 	"
            "CG_StockInD(nolock) D INNER JOIN CG_StockInM(nolock) M ON M.NID = D.StockInNID "
            "INNER JOIN B_GoodsSKU(nolock) AS bgs ON d.goodsSKuid = bgs.nid "
            "INNER JOIN b_goods(nolock) AS bg ON bg.nid = bgs.goodsid "
            "INNER JOIN b_goodsCats(nolock) as bgc on bgc.categoryCode = bg.categoryCode "
            "INNER JOIN B_Store(nolock) as bs on bs.nid = m.storeId "
            "WHERE  isnull(m.BillType,0)=2  "
            "AND CONVERT (VARCHAR (10),M.AudieDate, 121 ) BETWEEN '{}' and '{}' ")

        return sql.format(self.begin_date, self.end_date)

    def _get_transfer_change_out(self):
        """
        调拨出库明细:审核时间为准
        :return:
        """
        sql = (
            "SELECT   d.goodsSKuId AS sku_id, bgs.sku AS sku, bg.goodsCode AS goods_code, bg.goodsName AS goods_name,"
            "bgc.categoryname  as category_name, bgc.categoryParentName as  category_parent_name, "
            "bs.storename as store_name, m.AudieDate AS do_time, 'KC_StockChangeM.AudieDate' AS do_time_type, "
            "'调拨出库' AS do_type, d.nid AS do_source_entry_id, m.billNumber as do_source_bill_number, 	"
            "d.money AS do_amt,  	d.amount do_quantity FROM 	"
            "KC_StockChangeD(nolock) D INNER JOIN KC_StockChangeM(nolock) M ON M.NID = D.StockChangeNID "
            "INNER JOIN B_GoodsSKU(nolock) AS bgs ON d.goodsSKuid = bgs.nid "
            "INNER JOIN b_goods(nolock) AS bg ON bg.nid = bgs.goodsid "
            "INNER JOIN b_goodsCats(nolock) as bgc on bgc.categoryCode = bg.categoryCode "
            "INNER JOIN B_Store(nolock) as bs on bs.nid = m.StoreOutID "
            "WHERE m.CheckFlag = 1 "
            "AND CONVERT (VARCHAR (10), M.AudieDate, 121 ) BETWEEN '{}'  AND '{}'")

        return sql.format(self.begin_date, self.end_date)

    def _get_check_change_out(self):
        """
        盘点出库明细
        条件
        1. 审核
        2. 审核时间
        3. 盘出
        :return:
        """
        sql = (
            "SELECT   d.goodsSKuId AS sku_id, bgs.sku AS sku, bg.goodsCode AS goods_code, bg.goodsName AS goods_name,"
            "bgc.categoryname  as category_name, bgc.categoryParentName as  category_parent_name, "
            "bs.storename as store_name, m.AudieDate AS do_time, 'KC_StockCheckM.AudieDate' AS do_time_type, "
            "'盘点出库' AS do_type,'KC_StockCheckD' do_source_entry_table, d.nid AS do_source_entry_id, m.billNumber as do_source_bill_number, 	"
            "d.money AS do_amt,  	d.amount do_quantity FROM 	"
            "KC_StockCheckD(nolock) D INNER JOIN KC_StockCheckM(nolock) M ON M.NID = D.StockCheckNID "
            "INNER JOIN B_GoodsSKU(nolock) AS bgs ON d.goodsSKuid = bgs.nid "
            "INNER JOIN b_goods(nolock) AS bg ON bg.nid = bgs.goodsid "
            "INNER JOIN b_goodsCats(nolock) as bgc on bgc.categoryCode = bg.categoryCode "
            "INNER JOIN B_Store(nolock) as bs on bs.nid = m.storeId "
            "WHERE m.CheckFlag = 1 "
            " AND (IsNull(D.Amount,0)<0 or (IsNull(D.Amount,0)=0 and IsNull(D.money,0)<0)) "
            "AND CONVERT (VARCHAR (10), M.AudieDate, 121 ) BETWEEN '{}'  AND '{}'")

        return sql.format(self.begin_date, self.end_date)

    def _get_other_change_out(self):

        """
        其他出库明细：
        条件：
        1. 审核
        2. 审核时间
        3. 单据类型
        :return:
        """

        sql = (
            "SELECT   d.goodsSKuId AS sku_id, bgs.sku AS sku, bg.goodsCode AS goods_code, bg.goodsName AS goods_name,"
            "bgc.categoryname  as category_name, bgc.categoryParentName as  category_parent_name, "
            "bs.storename as store_name, m.AudieDate AS do_time, 'CK_StockOutD.AudieDate' AS do_time_type, "
            "'其他出库' AS do_type, 'CK_StockOutD' do_source_entry_table, d.nid AS do_source_entry_id, m.billNumber as do_source_bill_number, 	"
            "d.money AS do_amt,  	d.amount do_quantity FROM 	"
            "CK_StockOutD(nolock) D INNER JOIN CK_StockOutM(nolock) M ON M.NID = D.StockOutNID "
            "INNER JOIN B_GoodsSKU(nolock) AS bgs ON d.goodsSKuid = bgs.nid "
            "INNER JOIN b_goods(nolock) AS bg ON bg.nid = bgs.goodsid "
            "INNER JOIN b_goodsCats(nolock) as bgc on bgc.categoryCode = bg.categoryCode "
            "INNER JOIN B_Store(nolock) as bs on bs.nid = m.storeId "
            "WHERE  m.CheckFlag = 1"
            "AND CONVERT (VARCHAR (10), M.AudieDate, 121 ) BETWEEN '{}'  AND '{}' "
            " and ISNULL(m.BillType,0) =2 ")

        return sql.format(self.begin_date, self.end_date)

    def _get_sold_out(self):
        """
        销售出库
        条件：
        1. 审核
        2. 审核时间
        3. 单据类型
        :return:
        """
        sql = (
            "SELECT   d.goodsSKuId AS sku_id, bgs.sku AS sku, bg.goodsCode AS goods_code, bg.goodsName AS goods_name,"
            "bgc.categoryname  as category_name, bgc.categoryParentName as  category_parent_name, "
            "bs.storename as store_name, m.AudieDate AS do_time, 'CK_StockOutM.AudieDate' AS do_time_type, "
            "'销售出库' AS do_type, 'CK_StockOutD' do_source_entry_table, d.nid AS do_source_entry_id, m.billNumber as do_source_bill_number, 	"
            "d.money AS do_amt,  	d.amount do_quantity FROM 	"
            "CK_StockOutD(nolock) D INNER JOIN CK_StockOutM(nolock) M ON M.NID = D.StockOutNID "
            "INNER JOIN B_GoodsSKU(nolock) AS bgs ON d.goodsSKuid = bgs.nid "
            "INNER JOIN b_goods(nolock) AS bg ON bg.nid = bgs.goodsid "
            "INNER JOIN b_goodsCats(nolock) as bgc on bgc.categoryCode = bg.categoryCode "
            "INNER JOIN B_Store(nolock) as bs on bs.nid = m.storeId "
            "WHERE m.CheckFlag = 1 "
            "AND CONVERT (VARCHAR (10), 	M.AudieDate, 121 ) BETWEEN '{}'  AND '{}' AND isnull(BillType, 0) = 3 "
            " and  ISNULL(m.billnumber,'') like 'XSD%'  ")

        return sql.format(self.begin_date, self.end_date)

    def _put_data(self, rows, data_type, direction):
        sql = ("insert into report_sku_stock_change_detail (sku_id,sku,goods_code,goods_name,category_name,"
               "category_parent_name,store_name,do_time,do_time_type,do_type,do_source_entry_table,do_source_entry_id,"
               "do_source_bill_number,do_amt,do_quantity,created_time,updated_time) "
               "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),now())")

        try:
            cur = self.get_cur('erp')
            con = self.get_con('erp')
            cur.executemany(sql, rows)
            con.commit()
            self.logger.info(f'success to put data from {data_type} {direction} '
                             f'between {self.begin_date} and {self.end_date}')
        except Exception as why:
            self.logger.error(f'fail to put data from {data_type} {direction} '
                              f'between {self.begin_date} and {self.end_date} cause of {why}')

    def run(self):
        data_types = [DataType.purchased, DataType.purchasing, DataType.check, DataType.sold, DataType.other]
        for dt in data_types:
            self.data_trans(dt, 'in')
            self.data_trans(dt, 'out')


if __name__ == '__main__':
    worker = StockWorker(databases=['mssql', 'erp'], begin_date='2020-12-15', end_date='2020-12-30')
    worker.work()


