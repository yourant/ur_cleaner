#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-03-28 10:32
# Author: turpure


from ebaysdk.trading import Connection as Trading
from ebaysdk import exception
from src.services.base_service import BaseService
from configs.config import Config


class EbayTracker(BaseService):
    """
    get ebay order tracking info
    """

    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')

    def get_tracking(self):
        token = 'AgAAAA**AQAAAA**aAAAAA**PUFiXA**nY+sHZ2PrBmdj6wVnY+sEZ2PrA2dj6AGkIupD5eCpA6dj6x9nY+seQ**kykBAA**AAMAAA**iEMy8914/Jad1/soLAuhRzURlm2bNhDkE+dhj7zb0cc0d7L5rfKmGjwIMPS62d+2sFvsv6pEoZj3z+MfosfWTApVyUdYZ3qhyxgBxDRAbdAfzCZpDzLgmas00+15ZAQiwM0bIRu0H1r34utE8SZkhItyUuPhWP1e75B2iddiSy85EHUBbjRXOPlwypZozSms+iVXy3jZzLoKelGdwk5mrtQgm8s/BvxIEe83nkgHAygzm5fq9eqqAcXosdjY/zNK4DvBELPfVI4oNiae/5HrlBMs/Yh0ut4GIqnMDCtCQ+rXb/mk7hzngd8yxubr0gZkmL92a1ZmqXdfVpbnOtLF4CezaSjwYi8y0/Yxf6/TSY1ihIKCBTQ0GQPMpMt1kPmpusXN7KwK9iHJ32ShUoO1FSCZVR9nrE7KP7KCouTYYc5e6WBBz4Fm5QV2DzhsjmbB3PI8luzEVIX8OVXuHsd7kH6KsIU7tmDhFoLyz/dK7tWeTnR+0EOOKF+UQ8VmL3X/hOcTJqz5ZZJ4pOsVCJnAQCmmrCpM7jSVIC/RGarGClhGj+qcTleId+MBSRtzPRhg8KCACm04yXbvyeYQTvERvWVHBy8SLirRIo/iqORTOViuRLJtFxTvh6euJj9H4R8CRxKafTMq9d8qJMF0lcNQg0GJmL6FbRqKK4NNj55CiIYdK1bN01nRc5ovO6CzPThkAk0syQb9uEuO2eVzoEC3xq1Rmggy5SIJ0rM6uBlLqTHIjyqB48j4FMDH3xLQ2/x5'
        api = Trading(sited=0, config_file=self.config, timeout=40)
        par = {
            "RequesterCredentials": {"eBayAuthToken": token},
            # 'ItemTransactionIDArray': {"ItemTransactionID": {"TransactionID": "6T319518JT669080N"}}
            'OrderIDArray': {'OrderID': '283054740630-1962439790018'}
        }
        # response = api.execute('GetOrderTransactions', par)
        response = api.execute('GetOrders', par)
        return response.reply

    def work(self):
        try:
            ret = self.get_tracking()
            print(ret)
        except Exception as why:
            print(why)
        finally:
            self.close()


if __name__ == '__main__':
    tracker = EbayTracker()
    res = tracker.work()
    print(res)







