# -*- coding: utf-8 -*-
# @Time    : 2020/9/16 13:57
# @Author  : grassroadsZ
# @File    : 20-协程.py

import time
import functools


def print_run_time(func):
    @functools.wraps(func)
    def wraps(*args, **kwargs):
        s = time.perf_counter_ns()
        result = func(*args, **kwargs)
        e = time.perf_counter_ns()
        print(f"运行时间:{(e - s) / 10 ** 9} s")
        return result

    return wraps


def print_async_run_time(func):
    @functools.wraps(func)
    # 将内部wraps使用async装饰
    async def wraps(*args, **kwargs):
        s = time.perf_counter_ns()
        # 使用await进行函数调用
        result = await func(*args, **kwargs)
        e = time.perf_counter_ns()
        print(f"运行时间:{(e - s) / 10 ** 9} s")
        return result

    return wraps


def visit_url(url):
    print(f"visit {url}")
    sleep_time = int(url.split('_')[-1])
    time.sleep(sleep_time)
    print(f"Ok, {url}")


@print_run_time
def main(urls):
    for url in urls:
        visit_url(url)


# main(['visit_url_1', 'visit_url_2', 'visit_url_3'])
"""
运行结果:
visit visit_url_1
Ok, visit_url_1
visit visit_url_2
Ok, visit_url_2
visit visit_url_3
Ok, visit_url_3
运行时间:6.0020095 s
"""

# 协程写法
import asyncio


async def async_visit_url(url):
    print(f"visit {url}")
    sleep_time = int(url.split('_')[-1])
    await asyncio.sleep(sleep_time)
    print(f"Ok, {url}")


@print_async_run_time
async def async_main_1(urls):
    for url in urls:
        await async_visit_url(url)


# asyncio.run(async_main_1(['visit_url_1', 'visit_url_2', 'visit_url_3']))
"""
visit visit_url_1
Ok, visit_url_1
visit visit_url_2
Ok, visit_url_2
visit visit_url_3
Ok, visit_url_3
运行时间:6.0013056 s

"""


# 使用协程创建任务写法一
@print_async_run_time
async def async_main_2(urls):
    tasks = [asyncio.create_task(async_visit_url(url)) for url in urls]
    for task in tasks:
        await task


# asyncio.run(async_main_2(['visit_url_1', 'visit_url_2', 'visit_url_3']))

"""
运行结果
visit visit_url_1
visit visit_url_2
visit visit_url_3
Ok, visit_url_1
Ok, visit_url_2
Ok, visit_url_3
运行时间:3.0017149 s
"""

"""
使用协程创建任务写法二
1.asyncio.run(async_func()) 事件循环 开启
2.visit_url_1、2、3任务被创建运行到visit_url_1 的 print,执行await asyncio.sleep(sleep_time),从当前任务切出，切换至visit_url_2,当事件
visit_url_1 的await asyncio.sleep(sleep_time)的任务完成，事件调度器将控制权重新切换至visit_url_1 输出Ok, visit_url_1
3.当事件visit_url_2 的await asyncio.sleep(sleep_time)的任务完成，事件调度器将控制权重新切换至visit_url_2 输出Ok, visit_url_2，以此类推,
visit_url_3的 await asyncio.sleep(sleep_time)的任务完成，协程全任务结束，事件循环结束
注:return_exceptions=True 的作用是保证任务中的异常不会throw到执行层，为了避免使用try expect 来保证其它未被执行的任务被全部取消，需要设置为True
"""


@print_async_run_time
async def async_main_3(urls):
    tasks = [asyncio.create_task(async_visit_url(url)) for url in urls]

    await asyncio.gather(*tasks, return_exceptions=True)


# asyncio.run(async_main_3(['visit_url_1', 'visit_url_2', 'visit_url_3']))
"""
运行结果
visit visit_url_1
visit visit_url_2
visit visit_url_3
Ok, visit_url_1
Ok, visit_url_2
Ok, visit_url_3
运行时间:3.0012937 s
"""

"""
使用协程完成生产者消费者模式
"""

import random


async def consumer(queue, id):
    while True:
        val = await queue.get()
        print(f"{id} 获取到一个值为: {val}")
        await asyncio.sleep(1)


async def producer(queue, id):
    for i in range(5):
        val = random.randint(1, 10)
        await queue.put(val)
        print(f"{id} put了一个值为: {val}")
        await asyncio.sleep(1)


@print_async_run_time
async def run_main():
    queue = asyncio.Queue()
    consumer_1 = asyncio.create_task(consumer(queue, 'consumer_1'))
    consumer_2 = asyncio.create_task(consumer(queue, 'consumer_2'))

    producer_1 = asyncio.create_task(producer(queue, 'producer_1'))
    producer_2 = asyncio.create_task(producer(queue, 'producer_2'))

    await asyncio.sleep(5)
    consumer_1.cancel()
    consumer_2.cancel()

    await asyncio.gather(consumer_1, consumer_2, producer_1, producer_2, return_exceptions=True)


asyncio.run(run_main())
"""
运行结果
producer_1 put了一个值为: 4
producer_2 put了一个值为: 2
consumer_1 获取到一个值为: 4
consumer_2 获取到一个值为: 2
producer_1 put了一个值为: 8
producer_2 put了一个值为: 8
consumer_2 获取到一个值为: 8
consumer_1 获取到一个值为: 8
producer_1 put了一个值为: 7
producer_2 put了一个值为: 7
consumer_1 获取到一个值为: 7
consumer_2 获取到一个值为: 7
producer_1 put了一个值为: 8
producer_2 put了一个值为: 4
consumer_2 获取到一个值为: 8
consumer_1 获取到一个值为: 4
producer_1 put了一个值为: 1
producer_2 put了一个值为: 5
consumer_1 获取到一个值为: 1
consumer_2 获取到一个值为: 5
运行时间:5.0020019 s
"""
