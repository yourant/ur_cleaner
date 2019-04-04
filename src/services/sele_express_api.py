#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-04-03 15:55
# Author: turpure

from selenium import webdriver
from selenium.webdriver.common.proxy import Proxy, ProxyType
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

prox = Proxy()
prox.proxy_type = ProxyType.MANUAL

prox = Proxy()
prox.proxy_type = ProxyType.MANUAL
# prox.http_proxy = "127.0.0.1:1080"
# prox.socks_proxy = "127.0.0.1:1080"
prox.ssl_proxy = "175.155.249.57:8888"

capabilities = webdriver.DesiredCapabilities.CHROME
prox.add_to_capabilities(capabilities)

capabilities = webdriver.DesiredCapabilities.CHROME
prox.add_to_capabilities(capabilities)


def work():
    base_url = 'https://www.trackingmore.com/sprintpack-tracking/cn.html?number=0B048028400019917237E'
    options = webdriver.ChromeOptions()
    prefs = {"profile.managed_default_content_settings.images": 2,'javascript': 2,}
    options.add_experimental_option("prefs", prefs)
    options.add_experimental_option("excludeSwitches", ["ignore-certificate-errors"])
    options.add_argument('--disable-gpu')
    options.add_argument('--headless')
    chrome_driver_path = "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chromedriver.exe"
    driver = webdriver.Chrome(executable_path=chrome_driver_path,
                              chrome_options=options, desired_capabilities=capabilities)

    driver.get(base_url)
    try:
        element = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.TAG_NAME, "dd"))
        )
        dds = driver.find_elements_by_tag_name('dd')
        for ele in dds:
            print(ele.text)
    finally:
        driver.quit()


if __name__ == '__main__':
    work()