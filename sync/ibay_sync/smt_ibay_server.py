import requests
from configs.config import Config
import datetime
import pandas as pd



def login_session():
    base_url = 'http://139.196.109.214/index.php/myibay/login/redirect/%252Findex.php%252Fmyibay'
    config = Config()
    payload =  config.get_config(f'ibay_user_info')
    session = requests.Session()
    session.post(base_url, data=payload)
    return session





#导出SMT数据到excel表格
def generate(data, file_name):
    input_file = pd.DataFrame(data)
    try:
        input_file.to_excel(file_name, 'Sheet1', index=False)
    except Exception as why:
        print(why)
        pass





if __name__ == '__main__':
    se = login_session()
    res = se.get('http://139.196.109.214/index.php/joommanage/importpricequantity')
    print(res.content.decode('utf-8'))
