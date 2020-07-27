import requests
from configs.config import Config
import datetime
import pandas as pd


def login_session():
    base_url = 'http://139.196.109.214/index.php/myibay/login/redirect/%252Findex.php%252Fmyibay'
    config = Config()
    payload = config.get_config(f'ibay_user_info')
    session = requests.Session()
    session.post(base_url, data=payload)
    return session


def generate(data, file_name):
    # 导出SMT数据到excel表格
    # input_file = pd.DataFrame(data, index=[0])
    if type(data) is list:
        input_file = pd.DataFrame(data)
    else:
        input_file = pd.DataFrame(data, index=[0])
    try:
        input_file.to_excel(file_name, 'Sheet1', index=False)
    except Exception as why:
        print(why)
        pass


if __name__ == '__main__':
    se = login_session()
    res = se.get('http://139.196.109.214/index.php/joommanage/importpricequantity')
    print(res.content.decode('utf-8'))
