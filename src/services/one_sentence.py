#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-05-30 9:14
# Author: turpure

import requests
import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import platform
import time
import json
import re
from bs4 import BeautifulSoup
import codecs
import random


def get_sentence():
    """
    金山api
    :return:
    """
    today = str(datetime.datetime.now())[:10]
    # date = str(int(today[0:4]) - ) + today[4:]
    base_url = 'http://open.iciba.com/dsapi/?date={}'.format(today)
    try:
        res = requests.get(base_url)
        ret = res.json()
        return ret['content']
    except:
        return 'this is a beautiful day'


def get_quote():
    """
    天行api
    :return:
    """
    key = '186b6eeac08d0529bc3d073f50d776f3'
    base_url = 'http://api.tianapi.com/txapi/ensentence/?key={}'.format(key)
    try:
        res = requests.get(base_url)
        ret = res.json()
        return ret['newslist'][0]['en']
    except:
        return 'where amazing happens'


def get_you_dao():
    """
    有道每日一句
    :return:
    """

    plat = platform.system()
    if plat == 'Windows':
        DRIVER_PATH = r'C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe'
    else:
        DRIVER_PATH = '/usr/lib/chromium-browser/chromedriver'
    chrome_options = Options()
    pref = {"profile.managed_default_content_settings.images": 2}
    chrome_options.add_experimental_option("prefs", pref)
    # chrome_options.add_argument("--headless")
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--ignore-certificate-errors')
    base_url = 'http://dict.youdao.com'
    driver = webdriver.Chrome(DRIVER_PATH, chrome_options=chrome_options)
    try:
        driver.get(base_url)
        time.sleep(2)
        driver.execute_script('window.scrollBy(0,10000)')
        time.sleep(1)
        sentence_ele = driver.find_element_by_xpath('//span[contains(text(), "壹句")]/parent::div/parent::div/child::h3/child::a')
        print(sentence_ele.text)
        # sentence_ele = driver.find_element_by_css_selector('#vista > div:nth-child(3) > div > h3 > a')
        # sentence = sentence_ele.text
        # driver.close()
        # return sentence
    except Exception as why:
        print(why)
        return 'this is why we play'


def get_you_dao_api():
    today = str(datetime.datetime.now())[:10]
    base_url = ('http://dict.youdao.com/infoline/web?mode=publish&client=web&keyfrom=dict2.index&startDate={}'
                '&callback=vistaCallback').format(today)
    try:
        res = requests.get(base_url)
        ret = json.loads(re.findall(r'vistaCallback\((.*?)\)', res.text)[0])
        data = ret[today]
        for row in data:
            if row['type'] == '壹句':
                return row['title']
    except:
        return 'this is why we play'


def collect_one_piece():
    # base_url = 'http://www.yuluju.com/mingrenmingyan/11186.html'
    headers = {'User-Agent': 'PostmanRuntime/7.15.0'}
    base_url = 'http://www.pc841.com/yuedu/yulu/91213_all.html'
    ret = requests.get(base_url, headers=headers)
    soup = BeautifulSoup(ret.content.decode('utf-8'), 'lxml')
    p_elements = soup.find_all('p')
    with codecs.open('../../runtime/sentence.csv', 'a+', 'utf-8') as f:
        for ele in p_elements:
            print(ele.text)
            f.write(ele.text + '\n')


def get_one_piece():
    try:
        with codecs.open('../../runtime/sentence.csv', 'r', 'utf-8') as f:
            sentences = f.readlines()
            count = len(sentences)
            index = random.randint(0, count)
            ret = (sentences[index])
        with codecs.open('../../runtime/sentence.csv', 'w', 'utf-8') as f:
            for sec in sentences:
                if ret in sec:
                    continue
                f.write(sec)
        return ret
    except:
        return '要按照自己喜欢的去做，得不到别人的赞赏也没关系，不要怨恨自己出生的时代。'


if __name__ == "__main__":
    # collect_one_piece()
    print(get_you_dao_api())
