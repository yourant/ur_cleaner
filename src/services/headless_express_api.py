#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-04-03 13:41
# Author: turpure


import asyncio
from pyppeteer import launch
import re


def track_number():
    a = ['https://www.trackingmore.com/sprintpack-tracking/cn.html?number=0B048028400019917237E'] * 1
    for row in a:
        yield row


async def run(base_url):
    browser = await launch({'headless': False, 'disable-gpu': True})
    page = await browser.newPage()
    await page.goto(base_url)
    element = await page.querySelector('#trackItem_0 > tr > td > div.row_box_big.result-events > dl.row_box.origin')
    table = await page.evaluate('(element) => element.textContent', element)
    ret = re.sub(r'R(\d){2}', ';', table.strip())
    print(list(filter(lambda x: x != '', ret.split(';'))))


async def work():
    await asyncio.gather(*(run(url) for url in track_number()))


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(work())

