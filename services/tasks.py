#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-10-30 13:11
# Author: turpure

import json
from flask import Flask, request
from sync.haiying_spider.async_ebay_hot_product import Worker as HotWorker
from sync.haiying_spider.async_ebay_new_product import Worker as NewWorker
from sync.aliyun_image_search.image_worker import Worker as imageWorker
import asyncio

app = Flask(__name__)


@app.route('/recommend', methods=['POST'])
def get_ebay_recommend_products():
    ret = {}
    if request.method == 'POST':
        content = request.json
        rule_type = content.get('ruleType', '')
        rule_id = content.get('ruleId', '')
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        if rule_type == 'new':
            worker = NewWorker(rule_id)
            loop.run_until_complete(worker.run())
            ret = {'code': 200, 'message': 'success', 'data': []}
            pass
        if rule_type == 'hot':
            worker = HotWorker(rule_id)
            loop.run_until_complete(worker.run())
            ret = {'code': 200, 'message': 'success', 'data': []}
    else:
        ret = {'code': 400, 'message': 'only post method is allowed'}
    return json.dumps(ret)


@app.route('/image-search', methods=['POST'])
def image_search():
    ret = {}
    if request.method == 'POST':
        content = request.json
        image_url = content.get('imageUrl', '')
        worker = imageWorker()
        data = worker.request.search(image_url)
        ret = {'code': 200, 'message': 'success', 'data': json.loads(data)}

    else:
        ret = {'code': 400, 'message': 'only post method is allowed'}
    return json.dumps(ret)


if __name__ == "__main__":
    app.run(host='0.0.0.0')
