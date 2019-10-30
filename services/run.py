#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-10-30 14:15
# Author: turpure


from services.tasks import app

if __name__ == "__main__":
    app.run(host='0.0.0.0')
