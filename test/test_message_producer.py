#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-07-03 9:11
# Author: turpure
import pika
import time
import random
from pika import credentials
credentials = credentials.PlainCredentials('youran', 'youran2020')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='192.168.0.203', port=5672, credentials=credentials))

# connection = pika.BlockingConnection(
#     pika.ConnectionParameters(host='192.168.0.172'))
channel = connection.channel()

channel.queue_declare(queue='hello')

i = 0
while True:
    time.sleep(random.randint(1, 3))
    channel.basic_publish(exchange='', routing_key='hello', body=f'Hello {i}')
    i += 1
    print(f" [x] Sent 'Hello {i}'")

connection.close()

