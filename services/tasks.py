#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-10-30 13:11
# Author: turpure

import json
from flask import Flask, request
from sync.haiying_spider.ebay_hot_product import Worker as HotWorker
from sync.haiying_spider.ebay_new_product import Worker as NewWorker
app = Flask(__name__)


@app.route('/recommend', methods=['POST'])
def get_ebay_recommend_products():
    ret = {}
    if request.method == 'POST':
        content = request.json
        rule_type = content.get('ruleType', '')
        rule_id = content.get('ruleId', '')
        print(rule_type, rule_id)
        if rule_type == 'new':
            worker = NewWorker(rule_id)
            worker.run()
            ret = {'code': 200, 'message': 'success', 'data': []}
            pass
        if rule_type == 'hot':
            worker = HotWorker(rule_id)
            worker.run()
            ret = {'code': 200, 'message': 'success', 'data': []}
    else:
        ret = {'code': 400, 'message': 'only post method is allowed'}
    return json.dumps(ret)
