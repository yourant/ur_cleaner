#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-10-18 16:18
# Author: turpure


import time
import os
from multiprocessing import Pool


class A(object):
    def __init__(self, s):
        self.seed = s

    def sum(self, k):
        c = k
        start_t = time.time()

        for i in range(1000000):
          c += i * self.seed
        end_t = time.time()
        print(f'for loop sum takes {end_t - start_t} @{os.getpid()}')


class B(object):

    def __init__(self):
        self.a_list = []
        for i in range(10):
            self.a_list.append(A(i))

    def predict(self, k):
        result = []
        for a in self.a_list:
            result.append(a.sum(k))

    @staticmethod
    def _test(m):
        return m[0].sum(m[1])

    def mu_predict(self, k):
        with Pool(8) as pool:
            ret = pool.map(self._test, [(x, k) for x in self.a_list])

        return ret


if __name__ == '__main__':
    b = B()
    pd_begin = time.time()
    b.predict(10)
    pd_end = time.time()
    print(f'predict takes {pd_end-pd_begin}')

    mu_begin = time.time()
    b.mu_predict(10)
    mu_end = time.time()
    print(f'mu predict takes {mu_end - mu_begin}')