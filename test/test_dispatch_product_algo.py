#! usr/bin/env/python3
# coding:utf-8
# @Time: 2019-11-07 13:16
# Author: turpure


class Developer(object):
    def __init__(self, name, tag):

        self.name = name
        self.tag = tag
        self.limit = 1
        self.product = []
        self.pool = []


class Product(object):

    def __init__(self, name, ranking, tag):

        self.name = name
        self.ranking = ranking
        self.tag = tag
        self.limit = 0


def factory_developer(people):
    dev = []
    for pl in people:
        name = list(pl.keys())[0]
        tag = list(pl.values())[0]
        developer = Developer(name, tag)
        dev.append(developer)
    return dev


def factory_product(product):
    ret = []
    ranking = 0
    for pt in product:
        try:
            name = list(pt.keys())[0]
            tag = list(pt.values())[0]
            a_pt = Product(name, ranking, tag)
            ranking += 1
            ret.append(a_pt)
        except Exception as why:
            print(f'fail to create product cause of {why}')
    return ret


def gen_product():
    ret = []
    for index in range(1, 8):
        for tag in range(1, 5):
            ele = {f'p{index}{tag}': tag}
            ret.append(ele)
    return ret


def gen_people():
    ret = []
    name = ['A', 'B', 'C', 'D', 'E', 'C']
    tag = [1, 1, 1, 4, None, 3]
    for ne, tg in zip(name, tag):
        ele = {ne: tg}
        ret.append(ele)
    return ret


class Dispatcher(object):

    def __init__(self):
        people = gen_people()
        self.developer = factory_developer(people)
        product = gen_product()
        self.target = factory_product(product)

    def dispatch(self):

        # 有类目且类目相同的人挑
        ret = []
        same_dev = self.developer
        turn = 0
        while turn < 5:
            dev = self.turn_sort(same_dev, turn % 3)
            res = self.pick_up_matched_tag(dev)
            ret += res
            turn += 1

        # 没有类目的人轮流挑
        no_tag_dev = [self.developer[-2]]
        turn = 0
        while turn < 6:
            res = self.pick_up_unmatched_tag(no_tag_dev)
            turn += 1
            ret += res

        return ret

    def pick_up_matched_tag(self, dev):
        ret = []
        for dp in dev:
            for pt in self.target:
                condition1 = dp.limit <= 5 and pt.limit < 2
                condition2 = dp.tag
                condition3 = dp.tag == pt.tag
                condition4 = pt.name not in dp.pool

                # 开发有类目限制
                if condition1 and condition2 and condition3 and condition4:
                    dp.limit += 1
                    pt.limit += 1
                    dp.product.append((dp.name, pt.name, dp.tag, pt.tag))
                    dp.pool.append(pt.name)
                    print(f'{dp.name} 选中 {dp.name, pt.name, dp.tag, pt.tag}')
                    ret.append((dp.name, pt.name, dp.tag, pt.tag))
                    break
        return ret

    def pick_up_unmatched_tag(self, dev):
        ret = []
        for developer in dev:
            for pt in self.target:
                condition1 = developer.limit <= 5 and pt.limit < 2
                condition2 = not developer.tag
                condition4 = pt.name not in developer.pool

                # 开发有类目限制
                if condition1 and condition2 and condition4:
                    developer.limit += 1
                    pt.limit += 1
                    developer.product.append((developer.name, pt.name, developer.tag, pt.tag))
                    developer.pool.append(pt.name)
                    ret.append((developer.name, pt.name, developer.tag, pt.tag))
                    print(f'{developer.name} 选中 {developer.name, pt.name, developer.tag, pt.tag}')
                    break
        return ret

    @staticmethod
    def turn_sort(array, index):
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


if __name__ == "__main__":
    worker = Dispatcher()
    ret = worker.dispatch()
