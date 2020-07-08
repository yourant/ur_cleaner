#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-07-03 9:11
# Author: turpure

import pika
from pika import credentials

credentials = credentials.PlainCredentials('youran', 'youran2020')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='192.168.0.172', port=5672, credentials=credentials))
channel = connection.channel()

channel.queue_declare(queue='wish')


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body)


channel.basic_consume(
    queue='wish', on_message_callback=callback, auto_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
