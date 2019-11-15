#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-11-08 10:17
# Author: turpure


def sort(array, index):
    first = []
    left = []
    right = []
    length = len(array)
    for i in range(length):
        if i < index:
            left.append(array[i])
        elif i > index:
            right.append(array[i])
        else:
            first.append(array[i])
    return first + right + left


a = ['A', 'B', 'C']

turn = 0
while turn < 5:
    ret = sort(a, turn % 3)
    print(ret)
    turn += 1
