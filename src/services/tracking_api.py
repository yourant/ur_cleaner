#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-03-29 17:05
# Author: turpure

import requests
import json
from src.services import log


class Tracker(object):

    def __init__(self, track_number):
        self.logger = log.SysLogger().log
        action_req = ''
        express_name = ''
        self.base_url = ''
        self.track_number = track_number
        if track_number.startswith('WO'):
            express_name = 'winIt'
        if track_number.startswith('37'):
            express_name = 'winIt'
        if track_number.startswith('UE'):
            express_name = 'winIt'
        if track_number.startswith('0B'):
            express_name = 'sprint'
            action_req = 'rml'
        if track_number.startswith('VC'):
            express_name = 'sprint'
            action_req = 'rmr'

        self.express_name = express_name

        if express_name == 'winIt':
            self.base_url = 'http://track.winit.com.cn/tracking/Index/getTracking'
            self.data = {'trackingNoString': track_number}
        if express_name == 'sprint':
            self.base_url = 'http://track.sprintpack.com.cn/trackAPI.aspx'
            self.data = {'productbarcodes': track_number, 'actionReq': action_req}

    def track(self):
        out = {'trackNo': self.track_number, 'lastDate': '1990-01-01', 'lastDetail': ''}
        if self.base_url:
            res = requests.post(self.base_url, data=self.data)
            ret = json.loads(res.content)
            if self.express_name == 'winIt':
                try:
                    ret = ret['data']['transportation'][0]['trace'][-2]
                    out['lastDate'] = ret['date']
                    out['lastDetail'] = ret['eventStatus']
                except Exception as why:
                    self.logger.error(why)
            if self.express_name == 'sprint':
                try:
                    res = ret['lm_ts'][0]
                    out['lastDate'] = res['lastDate']
                    out['lastDetail'] = res['lastDetail']
                except Exception as why:
                    self.logger.error(why)
        return out


def test_17track():
    base_url = 'https://t.17track.net/restapi/track'
    # data = {"guid": "", "data": [{"num": "VC871014778GB", "fc": "190059"}], "timeZoneOffset": -480}
    data = {'{"guid":"","data":"'+ json.dumps([{"num":"VC871014778GB","fc":"190059"}]) + '","timeZoneOffset":-480}': ''}
    headers = {
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
               }
    ret = requests.post(base_url, data=json.dumps(data), headers=headers, verify=False)
    print(ret.content)


if __name__ == '__main__':
    tracker = Tracker('0B048028400021109013D')
    ret = tracker.track()
    print(ret)
    # test_17track()

