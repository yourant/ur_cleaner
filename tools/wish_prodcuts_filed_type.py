#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-07-06 13:37
# Author: turpure

import time
from concurrent.futures import ThreadPoolExecutor as Pool
from _datetime import datetime
from pymongo import MongoClient


mongo = MongoClient('192.168.0.150', 27017)
mongodb = mongo['operation']
col = mongodb['wish_products']


def find_all():
    ret = col.find({})
    for row in ret:
        yield row


def set_type(row):
    try:
        updated_time = row['last_updated']
        if not isinstance(updated_time, int):
            updated_time_structure = time.strptime(updated_time, "%m-%d-%YT%H:%M:%S")
            updated_stamp = int(time.mktime(updated_time_structure))
        else:
            updated_stamp = updated_time

        date_uploaded = row['date_uploaded']
        if not isinstance(date_uploaded, int):
            date_uploaded_time_structure = time.strptime(date_uploaded, "%m-%d-%Y")
            date_uploaded_stamp = int(time.mktime(date_uploaded_time_structure))
        else:
            date_uploaded_stamp = date_uploaded
        shipping_price = float(row.get('default_shipping_price', 0))
        number_saves = int(row['number_saves'])
        number_sold = int(row['number_sold'])
        local_shipping_price = float(row.get('localized_default_shipping_price', 0))
        update_statement = {
                'last_updated': updated_stamp, 'date_uploaded': date_uploaded_stamp,
                'default_shipping_price': shipping_price, 'localized_default_shipping_price': local_shipping_price,
                'number_saves': number_saves, 'number_sold': number_sold
             }
        col.update_one({'_id': row['_id']}, {
            '$set': update_statement
                })
        # print(f'updating {row["_id"]} ')
    except Exception as why:
        print(f'fail to update {row["_id"]} cause of {why}')


def set_date_formatter(row):
    try:
        updated_time = row['last_updated']
        if isinstance(updated_time, int):
            if len(str(updated_time)) == 10:
                updated_time = datetime.fromtimestamp(updated_time)
            if len(str(updated_time)) == 13:
                updated_time = datetime.fromtimestamp(updated_time / 1000)
        date_uploaded = row['date_uploaded']
        if isinstance(date_uploaded, int):
            if len(str(date_uploaded)) == 10:
                date_uploaded = datetime.fromtimestamp(date_uploaded)
            if len(str(date_uploaded)) == 13:
                date_uploaded = datetime.fromtimestamp(date_uploaded / 1000)
        update_statement = {
            'last_updated': updated_time, 'date_uploaded': date_uploaded,
        }
        col.update_one({'_id': row['_id']}, {
            '$set': update_statement
        })
        # print(f'updating {row["_id"]}')
    except Exception as why:
        print(f'fail to update {row["_id"]} cause of {why}')


def main():
    try:
        rows = find_all()
        for ele in rows:
            set_type(ele)
    except Exception as why:
        print(f'mission failed cause of {why}')
    finally:
        mongo.close()


def run():
    try:
        rows = find_all()
        with Pool(32) as pl:
            pl.map(set_date_formatter, rows)
    except Exception as why:
        print(f'mission failed cause of {why}')
    finally:
        mongo.close()


if __name__ == '__main__':
    begin = time.time()
    run()
    end = time.time()
    print(f' it takes: {end - begin}')
