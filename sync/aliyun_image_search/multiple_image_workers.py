#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-11-26 11:08
# Author: turpure


from sync.aliyun_image_search.base_request import BaseRequest
from src.services.base_service import BaseService
from pymongo import MongoClient
from multiprocessing import Pool

mongo = MongoClient('192.168.0.150', 27017)
mongodb = mongo['product_engine']
col = mongodb['images_tasks']
base_request = BaseRequest()


def get_images():
    images = col.find({"doneFlag": 0})
    for row in images:
        row['process'] = 'transaction'
        yield row


def mark_image_task(image, status):
    try:
        id = image['_id']
        col.find_one_and_update({"_id": id}, {"$set": {"doneFlag": status}})
        print(f'success to finish task {image["sku"]}')
    except Exception as why:
        print(f'fail to save task {image["sku"]} cause of {why}')


def transaction(img):
    try:
        result = base_request.add(image_url=img['img'], image_name=img['sku'])
        mark_image_task(img, status=result)
    except Exception as why:
        print(f'fail to finish image transaction because of {why}')


def start():
    try:
        images = get_images()
        with Pool(5) as p:
            p.map(transaction, images)

    except Exception as why:
        print(f'fail to run image-worker cause of {why}')
    finally:
        mongo.close()


if __name__ == '__main__':
    start()



