#! usr/bin/env/python3
# coding:utf-8
# @Time: 2020-03-24 14:23
# Author: turpure


import click
from sync.tupianku_image_delete.delete_tupianku_image import Worker as Cleaner

@click.command()
@click.option('--name',default='2',help='图片库编号 参数，非必须，默认值为图片库2')
@click.option('--push',default='yes',help='是否从普源推送新任务，非必须。默认值为yes')
def work(name, push):
    cleaner = Cleaner(tupianku_name=name)
    if push != 'yes':
        cleaner.run()
    else:
        cleaner.push_tasks()
        cleaner.run()


if __name__ == '__main__':
    work()
