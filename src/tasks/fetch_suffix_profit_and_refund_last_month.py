#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-05-14 13:04
# Author: turpure

import datetime
from src.tasks.fetch_suffix_profit import ProfitFetcher
from src.tasks.fetch_suffix_refund import RefundFetcher


def get_last_month_first_day():
    td = datetime.date.today()
    year = td.year
    month = td.month
    if month == 1:
        year -= 1
        month = 12
    else:
        month -= 1
    return datetime.date(year, month, 1)

def get_last_month_end_day():
    td = datetime.date.today()
    year = td.year
    month = td.month
    return datetime.date(year, month, 1) - datetime.timedelta(1)


if __name__ == "__main__":
    last_month_first_day = str(get_last_month_first_day())[:10]
    last_month_end_day = str(get_last_month_end_day())[:10]


    # fetch suffix profit
    profit_worker = ProfitFetcher()
    profit_worker.work(last_month_first_day, last_month_end_day)
    #
    # fetch suffix refund
    refund_worker = RefundFetcher()
    refund_worker.work(last_month_first_day, last_month_end_day)
