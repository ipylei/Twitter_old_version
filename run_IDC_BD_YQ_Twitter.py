# -*- coding: utf-8 -*-
import datetime
import os
import time
import logging
# from multiprocessing import Process

from apscheduler.schedulers.blocking import BlockingScheduler

from twitter.bloom_filter import RedisClient
from twitter.settings import REDIS_KEY_URLS_STATUS
from twitter.twitter_relations import Twitter


def delay(seconds=None, minutes=None, hours=None, days=None):
    current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    with open('end_time.txt', 'a+', encoding='utf-8') as f:
        f.write('采集结束：' + current_time + '\n')

    if seconds:
        logging.warning(
            f'{current_time}，本轮采集已完成,{seconds}秒后进行开始下一轮采集.......................................................')
        time.sleep(seconds)
    elif minutes:
        interval = minutes * 60
        logging.warning(
            f'{current_time}，本轮采集已完成,{minutes}分钟后进行开始下一轮采集.......................................................')
        time.sleep(interval)
    elif hours:
        interval = hours * 60 * 60
        logging.warning(
            f'{current_time}，本轮采集已完成,{hours}小时后进行开始下一轮采集.......................................................')
        time.sleep(interval)
    elif days:
        interval = days * 24 * 60 * 60
        logging.warning(
            f'{current_time}，本轮采集已完成,{days}天后进行开始下一轮采集.......................................................')
        time.sleep(interval)


def start_consume_relations(status):
    while not status:  # 社交关系通道是否存活(不存在就消费直到遇见pipelines中推过来的is_end, 存活就不再消费)
        try:
            twitter_obj = Twitter()
            twitter_obj.login()
            is_end = twitter_obj.start_crawl()
            if is_end:  # 不再从redis中获取社交关系
                print('本轮社交关系获取完毕...')
                break
            else:
                # 速率被限制？休眠900s
                time.sleep(900)
        except Exception as e:
            print(e)
            pass
        print('即将重新登录...')
    print('今天已采集完社交关系...')


def first_crawl(**kwargs):
    current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    with open('end_time.txt', 'a+', encoding='utf-8') as f:
        f.write('首次采集开始时间：' + current_time + '\n')

    # redis_client = RedisClient()
    # status = redis_client.db.get(REDIS_KEY_URLS_STATUS)
    # redis_client.close()
    # Process(target=start_consume_relations, args=(status,)).start()
    status = 1
    if status:
        os.system("scrapy crawl twitter_spider -a is_increment_crawl=0 -a status=1")
    else:
        os.system("scrapy crawl twitter_sdpider -a is_increment_crawl=0 -a status=0")
    delay(**kwargs)
    pass


def increment_crawl(**kwargs):
    while True:
        current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        with open('end_time.txt', 'a+', encoding='utf-8') as f:
            f.write('增量采集开始时间：' + current_time + '\n')

        # redis_client = RedisClient()
        # status = redis_client.db.get(REDIS_KEY_URLS_STATUS)
        # redis_client.close()
        # Process(target=start_consume_relations, args=(status,)).start()
        status = 1
        if status:
            os.system("scrapy crawl twitter_spider -a is_increment_crawl=1 -a status=1")
        else:
            os.system("scrapy crawl twitter_spider -a is_increment_crawl=1 -a status=0")
        delay(**kwargs)
        pass


def increment_crawl_spider(**kwargs):
    current_start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    with open('end_time.txt', 'a+', encoding='utf-8') as f:
        f.write('增量采集开始时间：' + current_start_time + '\n')
    # redis_client = RedisClient()
    # status = redis_client.db.get(REDIS_KEY_URLS_STATUS)
    # redis_client.close()
    # Process(target=start_consume_relations, args=(status,)).start()
    status = 1
    if status:
        os.system("scrapy crawl twitter_spider -a is_increment_crawl=1 -a status=1")
    else:
        os.system("scrapy crawl twitter_spider -a is_increment_crawl=1 -a status=0")
    current_end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    with open('end_time.txt', 'a+', encoding='utf-8') as f:
        f.write('采集结束：' + current_end_time + '\n')
    print(f'{current_end_time}，本轮采集已完成.......................................................')


def timed_task():
    # 创建调度器：BlockingScheduler
    scheduler = BlockingScheduler()
    # 添加任务,时间间隔
    scheduler.add_job(increment_crawl_spider, trigger='interval', minutes=5, kwargs={'i': 2}, max_instances=10,
                      next_run_time=datetime.datetime.now())
    scheduler.start()


if __name__ == '__main__':
    pass
    # first_crawl(minutes=5)
    # increment_crawl(minutes=5)

    # timed_task()
    # increment_crawl_spider()

    from scrapy import cmdline

    cmdline.execute("scrapy crawl twitter_spider -a is_increment_crawl=0 -a status=1".split())
    # cmdline.execute("scrapy crawl twitter_spider -a is_increment_crawl=0 -a status=0".split())
    # cmdline.execute("scrapy crawl twitter_spider -a is_increment_crawl=1 -a status=1".split())
    # cmdline.execute("scrapy crawl twitter_spider -a is_increment_crawl=1 -a status=0".split())
