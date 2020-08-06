#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-07-03 9:11
# Author: turpure
import pika
import time
import random
import json
from pika import credentials
credentials = credentials.PlainCredentials('youran', 'youran2020')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='192.168.0.203', port=5672, credentials=credentials))

# connection = pika.BlockingConnection(
#     pika.ConnectionParameters(host='192.168.0.172'))

queue_name = 'wish-pb'
channel = connection.channel()

channel.queue_declare(queue=queue_name)
# body = {'request-url': 'wish-update', 'callback-url' 'data': {'param': ''}}
body =  b'{"request":{"url":"https:\\/\\/merchant.wish.com\\/api\\/v2\\/product-boost\\/campaign\\/create?access_token=9a5be73c7dc042afb173dac8389edfc5","method":"post","type":"json","params":"{\\"access_token\\":\\"9a5be73c7dc042afb173dac8389edfc5\\",\\"max_budget\\":5,\\"auto_renew\\":true,\\"campaign_name\\":\\"pb_7H0572@#E258\\",\\"start_date\\":\\"2020-08-06\\",\\"end_date\\":\\"2020-08-08\\",\\"products\\":[{\\"product_id\\":\\"5eec3483d1b31500d98a9533\\"}],\\"scheduled_add_budget_enabled\\":true,\\"scheduled_add_budget_days\\":[\\"1\\",\\"2\\"],\\"scheduled_add_budget_amount\\":\\"2\\",\\"intense_boost\\":true}"},"callback":{"url":"http:\\/\\/192.168.0.150:18881\\/v1\\/operation-wish\\/finish-wish-product-boost-task","method":"post","type":"json","params":{"access_token":"_S0p_sogFoZoyxbygrmznCwN6IQ7ORgD_1594016021","condition":{"taskId":"5f2a75f9610bf16534524c63","response":""}}},"timestamp":1596618233}'
body = json.loads(body)
i = 0
while True:
    body['timestamp'] = int(time.time() * 1000)
    time.sleep(random.randint(1, 3))
    channel.basic_publish(exchange='', routing_key=queue_name, body=json.dumps(body))
    i += 1
    print(f" [x] Sent 'Hello {i}'")

connection.close()

