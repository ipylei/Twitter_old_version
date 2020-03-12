# -*- coding: utf-8 -*-
import datetime
import json
import os
import threading
import time
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Process

import requests
from apscheduler.schedulers.blocking import BlockingScheduler

from twitter.bloom_filter import RedisClient
from twitter.settings import PlatformName, ProjectName, USER_ACCOUNTS_URL, REDIS_KEY_URLS_STATUS
from twitter.twitter_relations import Twitter

headers = {
    'Connection': 'keep-alive',
    'Content-Length': '55',
    'accept': '*/*',
    'Origin': 'http://36.110.98.20:9041',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
    'Content-Type': 'application/json',
    'Referer': 'http://36.110.98.20:9041/swagger-ui.html',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cookie': 'theme-skin=white&/css/theme/white.css',
}
body = {
    "PlatformName": PlatformName,
    "ProjectName": ProjectName
}


# def get_account():
#     response = requests.post(url=USER_ACCOUNTS_URL, data=json.dumps(body), headers=headers, verify=False)
#     task = json.loads(response.text)
#     user_list = task.get('ListTask')
#     print(user_list)
#     print(len(threading.enumerate()))
#     time.sleep(100)
#     print('hello world')
#
#
# def timed_task():
#     # 创建调度器：BlockingScheduler
#     scheduler = BlockingScheduler()
#     # 添加任务,时间间隔
#     scheduler.add_job(get_account, trigger='interval', seconds=5, max_instances=10,
#                       next_run_time=datetime.datetime.now())
#     scheduler.start()


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
        os.system("scrapy crawl twitter_spider_temp -a is_increment_crawl=1 -a status=1")
    else:
        os.system("scrapy crawl twitter_spider_temp -a is_increment_crawl=1 -a status=0")
    current_end_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    with open('end_time.txt', 'a+', encoding='utf-8') as f:
        f.write('采集结束：' + current_end_time + '\n')
    print(f'{current_end_time}，本轮采集已完成.......................................................')


if __name__ == '__main__':
    # timed_task()
    pass

    with ProcessPoolExecutor(5) as executor:
        while True:
            user_accounts_path = "./user_accounts_temp.json"
            if not os.path.exists(user_accounts_path):
                with open(user_accounts_path, 'w', encoding='utf-8') as f:
                    data = {"accounts": []}
                    json.dump(data, f, ensure_ascii=False)

            # (?)秒获取一次请求
            try:
                response = requests.post(url=USER_ACCOUNTS_URL, data=json.dumps(body), headers=headers, verify=False)
                task = json.loads(response.text)
                user_list = task.get('ListTask')
            except Exception as e:
                print(e)
                time.sleep(5)
                continue

            # 加载本地账号信息
            with open(user_accounts_path, 'r+', encoding='utf-8') as fb:
                user_accounts = json.load(fb).get("accounts")
                user_accounts_list = []
                if user_accounts:
                    for user in user_accounts:
                        user_accounts_list.append(user.get("Url"))
                # 更新本地账号信息
                fb.seek(0)
                fb.truncate()
                data = {"accounts": user_list}
                json.dump(data, fb, ensure_ascii=False)

            new_user_count = 0
            new_user_list = []
            if user_list:
                for user in user_list:
                    user_account = user.get("Url")
                    if user_account not in user_accounts_list:
                        # 表示为新增的账号d
                        user["is_new"] = True
                        new_user_count += 1
                        new_user_list.append(user)
            current_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            if new_user_count:
                print(current_time, "有新增账号...")
                with open("user_accounts_new.json", "w") as fc:
                    data = {"accounts": new_user_list}
                    json.dump(data, fc, ensure_ascii=False)
                executor.submit(increment_crawl_spider, kwargs={'has_new': True})
                time.sleep(30)
            else:
                print(current_time, "无新增账号, 5秒后再次获取...")
                time.sleep(5)

    # from scrapy import cmdline
    #
    # cmdline.execute("scrapy crawl twitter_spider_temp -a is_increment_crawl=1 -a status=1".split())
