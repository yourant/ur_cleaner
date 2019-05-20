#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-05-17 17:47
# Author: turpure

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time

DRIVER_PATH = r'C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe'
chrome_options = Options()
pref = {"profile.managed_default_content_settings.images": 2}
chrome_options.add_experimental_option("prefs", pref)
chrome_options.add_argument("--headless")
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--proxy-server=http://127.0.0.1:8888')
chrome_options.add_argument('--ignore-certificate-errors')


def get_token():
    base_url = 'https://www.joom.com/en/products/5cd3bd251436d4010106b2d1'
    driver = webdriver.Chrome(DRIVER_PATH, chrome_options=chrome_options)
    driver.get(base_url)
    time.sleep(10)
    driver.close()


if __name__ == '__main__':
    get_token()
