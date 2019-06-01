#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-05-30 10:13
# Author: turpure


from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import platform
from src.services.one_sentence import get_you_dao_api
from configs.config import Config

yii_user_info = Config().get_config('yii')
plat = platform.system()
if plat == 'Windows':
    DRIVER_PATH = r'C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe'
else:
    DRIVER_PATH = '/usr/lib/chromium-browser/chromedriver'
chrome_options = Options()
pref = {"profile.managed_default_content_settings.images": 2}
chrome_options.add_experimental_option("prefs", pref)
chrome_options.add_argument("--headless")
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--ignore-certificate-errors')


def sign_in(username, password, what_to_say):
    base_url = 'https://www.yiichina.com/login'
    driver = webdriver.Chrome(DRIVER_PATH, chrome_options=chrome_options)
    driver.get(base_url)

    # login
    user_name_ele = driver.find_element_by_id('loginform-username')
    password_ele = driver.find_element_by_id('loginform-password')
    login_btn = driver.find_element_by_name('login-button')
    user_name_ele.send_keys(username)
    password_ele.send_keys(password)
    login_btn.click()
    time.sleep(5)

    # sing in
    try:
        registration_btn = driver.find_element_by_xpath('//a[contains(@class, "btn-registration")]')
        registration_btn.click()
        time.sleep(2)
    except Exception as why:
        print(why)

    # write one sentence
    try:
        feed_ele = driver.find_element_by_id('feed-content')
        publish_btn = driver.find_element_by_xpath('//button[contains(text(), "发布")]')
        feed_ele.send_keys(what_to_say)
        publish_btn.click()
        time.sleep(1)
    except Exception as why:
        print(why)

    finally:
        driver.close()


if __name__ == "__main__":
    say = get_you_dao_api()
    sign_in(yii_user_info['username'], yii_user_info['password'], say)
