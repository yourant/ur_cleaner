#! usr/bin/env/python3
# coding:utf-8
# @Time: 2018-10-19 16:28
# Author: turpure

from time import strftime, localtime
from datetime import timedelta, date
import calendar


year = strftime("%Y", localtime())
mon = strftime("%m", localtime())
day = strftime("%d", localtime())
hour = strftime("%H", localtime())
mini = strftime("%M", localtime())
sec = strftime("%S", localtime())


def today():
    """
    get today,date format="YYYY-MM-DD"
    """
    return date.today()


def today_str():
    """
    get date string, date format="YYYYMMDD"
    """
    return year+mon+day


def datetime():
    """''
    get datetime,format="YYYY-MM-DD HH:MM:SS"
    """
    return strftime("%Y-%m-%d %H:%M:%S",localtime())


def datetime_str():
    """''
    get datetime string
    date format="YYYYMMDDHHMMSS"
    """
    return year+mon+day+hour+mini+sec


def get_day_of_day(n=0):
    """
    if n>=0,date is larger than today
    if n<0,date is less than today
    date format = "YYYY-MM-DD"
    """
    if n < 0:
        n = abs(n)
        return date.today()-timedelta(days=n)
    else:
        return date.today()+timedelta(days=n)


def get_days_of_month(year, mon):
    """
    get days of month
    """
    return calendar.monthrange(year, mon)[1]


def get_first_day_of_month(year, mon):
    """
    get the first day of month
    date format = "YYYY-MM-DD"
    """
    days = "01"
    if int(mon) < 10:
        mon = "0"+str(int(mon))
    arr = (year, mon, days)
    return "-".join("%s" %i for i in arr)


def get_last_day_of_month(year, mon):
    """
    get the last day of month
    date format = "YYYY-MM-DD"
    """
    days = calendar.monthrange(year, mon)[1]
    mon = add_zero(mon)
    arr = (year, mon, days)
    return "-".join("%s" %i for i in arr)


def get_first_day_month(n=0):
    """''
    get the first day of month from today
    n is how many months
    """
    (y, m, d) = get_year_and_month(n)
    d = "01"
    arr = (y, m, d)
    return "-".join("%s" % i for i in arr)


def get_last_day_month(n=0):
    """''
    get the last day of month from today
    n is how many months
    """
    return "-".join("%s" % i for i in get_year_and_month(n))


def get_year_and_month(n=0):
    """''
    get the year,month,days from today
    before or after n months
    """
    this_year = int(year)
    this_mon = int(mon)
    total_mon = this_mon+n
    if n >= 0:
        if total_mon <= 12:
            days = str(get_days_of_month(this_year,total_mon))
            total_mon = add_zero(total_mon)
            return year, total_mon, days
        else:
            i = total_mon/12
            j = total_mon%12
            if j == 0:
                i -= 1
                j = 12
            this_year += i
            days = str(get_days_of_month(this_year,j))
            j = add_zero(j)
            return str(this_year), str(j), days
    else:
        if total_mon > 0 & total_mon <12:
            days = str(get_days_of_month(this_year,total_mon))
            total_mon = add_zero(total_mon)
            return year, total_mon, days
        else:
            i = total_mon / 12
            j = total_mon % 12
            if j == 0:
                i -= 1
                j = 12
            this_year += i
            days = str(get_days_of_month(this_year,j))
            j = add_zero(j)
            return str(this_year), str(j), days


def add_zero(n):
    """''
    add 0 before 0-9
    return 01-09
    """
    nabs = abs(int(n))
    if nabs<10:
        return "0"+str(nabs)
    else:
        return nabs


def get_today_month(n=0):

    (y, m, d) = get_year_and_month(n)
    arr = (y, m, d)
    if int(day) < int(d):
        arr = (y, m, day)
    return "-".join("%s" % i for i in arr)


def time_range(m, n):

    for i in range(m, n):
        time_list = []
        t1 = get_day_of_day(-i)
        time_list.append(str(t1))
        t2 = get_day_of_day(-i+1)
        time_list.append(str(t2))
        yield time_list


def monthrange(m, n):
    """GET THE DATE ERVEY 30 DAYS"""
    for i in range(m, n):
        time_list = []
        t1 = get_day_of_day(-i*30)
        time_list.append(str(t1))
        t2 = get_day_of_day(-i*30+30)
        time_list.append(str(t2))
        yield time_list


if __name__ == "__main__":
    for i in monthrange(1, 2):
        print(i)


