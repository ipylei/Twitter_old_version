# -*- coding: utf-8 -*-

import json
import logging
import random
import re
import time

import pika
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from twitter.bloom_filter import RedisClient, BloomFilter

from twitter.settings import *


class Twitter(object):

    def __init__(self):
        chrome_options = webdriver.ChromeOptions()
        # # 1.设置无头模式
        # chrome_options.add_argument('--headless')
        # # 2.设置chromedriver的执行路径
        # path = "./chromedriver.exe"  # 注意这个路径需要时可执行路径（chmod 777 dir or 755 dir）
        self.browser = webdriver.Chrome(chrome_options=chrome_options)

        # self.browser = webdriver.Chrome()
        # self.browser.maximize_window()
        self.wait = WebDriverWait(self.browser, 30)

        self.db_key_relations = DB_KEY_RELATIONS

        self.username = USERNAME
        self.password = PASSWORD
        self.telephone = TELEPHONE

        self.mq_host = MQ_HOST
        self.mq_port = MQ_PORT
        self.mq_username = MQ_USERNAME
        self.mq_password = MQ_PASSWORD
        self.mq_relations_queue = MQ_QUEUE_RELATIONS

        self.redis_host = REDIS_HOST
        self.redis_port = REDIS_PORT
        self.redis_db = REDIS_DB
        self.redis_key_urls = REDIS_KEY_URLS  # 传过来的列表
        self.redis_client = RedisClient(host=self.redis_host, port=self.redis_port, redis_db=self.redis_db)

        self.bloom_key = REDIS_KEY_URLS_BLOOM
        self.bloom = BloomFilter(server=self.redis_client, bloom_key=self.bloom_key)

        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.mq_host, port=self.mq_port,
                                      credentials=pika.PlainCredentials(self.mq_username, self.mq_password),
                                      heartbeat=0))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.mq_relations_queue, durable=False)  # 社交关系队列

    def __del__(self):
        self.browser.close()  # 关闭浏览器
        self.redis_client.close()  # 关闭redis
        self.connection.close()

    def login(self):
        self.browser.get("https://twitter.com/login")
        xpath_input_username = "//fieldset//input[@name='session[username_or_email]']"
        xpath_input_password = "//fieldset//input[@name='session[password]']"
        xpath_button_login = "//button[@type='submit']"

        input_username = self.wait.until(EC.presence_of_element_located((By.XPATH, xpath_input_username)))
        input_password = self.wait.until(EC.presence_of_element_located((By.XPATH, xpath_input_password)))
        button_login = self.wait.until(EC.presence_of_element_located((By.XPATH, xpath_button_login)))
        input_username.clear()
        input_username.send_keys(self.username)
        input_password.clear()
        input_password.send_keys(self.password)
        button_login.click()
        try:
            # 登录成功
            self.wait.until(EC.presence_of_element_located((By.XPATH, "//header[@role='banner']")))
            print('登录成功!')
        except Exception as e:
            print(e)
            # 异地登录(输入手机号？)
            input_telephone = self.wait.until(EC.presence_of_element_located((By.XPATH, "//input[@type='text']")))
            button_submit = self.wait.until(EC.presence_of_element_located((By.XPATH, "//input[@type='submit']")))
            # 输入手机号
            input_telephone.clear()
            input_telephone.send_keys(self.telephone)
            button_submit.click()
            self.wait.until(EC.presence_of_element_located((By.XPATH, "//header[@role='banner']")))
            print('登录成功!')

    def start_crawl(self):
        max_failure_time = 0
        wait_time = 0
        while True:
            if max_failure_time >= 20:
                print('失败达到最大次数，即将重新登录...')
                return
            # 从redis中获取数据
            if self.redis_client.empty(self.redis_key_urls):
                print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())), '5秒后再次获取...')
                time.sleep(5)  # 次/5S
                wait_time += 5
                if wait_time <= 300:  # 5分钟后若还未数据断开连接
                    continue
                is_end = 1
                return is_end
            article_json = self.redis_client.pop(self.redis_key_urls)
            article_dct = json.loads(article_json)
            is_end = article_dct.get("is_end")
            if is_end:
                print(is_end)
                print('当前循环已完成,准备退出...')
                return is_end
            print('获取到Redis数据：', article_dct)
            article_id = article_dct["article_id"]
            article_url = article_dct["article_url"]

            try:
                self.browser.get(article_url)
                xpath_js_file = "//head//link[@rel='preload'][contains(@href,'/main.')]"
                js_element = self.wait.until(EC.presence_of_element_located((By.XPATH, xpath_js_file)))
                js_file_url = js_element.get_attribute("href")
                js_content_response = requests.get(url=js_file_url, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.75 Safari/537.36'
                })
                authorization = re.search('i="ACTION_REFRESH"\S*?s="(.*?)"', js_content_response.text).group(1)
                authorization = "Bearer " + authorization
                # 获取当前页面的cookies
                cookies = dict([(cookie["name"], cookie["value"]) for cookie in self.browser.get_cookies()])
                token = cookies.get("ct0")
                common_headers = {'Host': 'api.twitter.com',
                                  'Connection': 'keep-alive',
                                  'Sec-Fetch-Mode': 'cors',
                                  'Origin': '//twitter.com',
                                  'x-twitter-client-language': 'en',
                                  'x-csrf-token': token,
                                  'authorization': authorization,
                                  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.75 Safari/537.36',
                                  'x-twitter-auth-type': 'OAuth2Session',
                                  'x-twitter-active-user': 'yes', 'Accept': '*/*',
                                  'Sec-Fetch-Site': 'same-site',
                                  'Referer': "",
                                  'Accept-Language': 'zh-CN,zh;q=0.9'}

                # TODO 转推人页面
                # retweets_url = f"https://api.twitter.com/2/timeline/retweeted_by.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_composer_source=true&include_ext_alt_text=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweets=true&tweet_id={article_id}&count=80&ext=mediaStats%2ChighlightedLabel%2CcameraMoment"
                retweets_url = f"https://api.twitter.com/2/timeline/retweeted_by.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_composer_source=true&include_ext_alt_text=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweets=true&tweet_id={article_id}&ext=mediaStats%2ChighlightedLabel%2CcameraMoment"
                common_headers["Referer"] = article_url + '/retweets'
                response_retweets = requests.get(url=retweets_url, headers=common_headers, cookies=cookies).json()

                # TODO 点赞人页面
                # likes_url = f"https://api.twitter.com/2/timeline/liked_by.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_composer_source=true&include_ext_alt_text=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweets=true&tweet_id={article_id}&count=80&ext=mediaStats%2ChighlightedLabel%2CcameraMoment"
                likes_url = f"https://api.twitter.com/2/timeline/liked_by.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_composer_source=true&include_ext_alt_text=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweets=true&tweet_id={article_id}&ext=mediaStats%2ChighlightedLabel%2CcameraMoment"
                common_headers["Referer"] = article_url + '/likes'
                response_likes = requests.get(url=likes_url, headers=common_headers, cookies=cookies).json()

            except Exception as e:
                max_failure_time += 1
                logging.error(e)
            else:
                # TODO 添加/更新相关人物
                # self.add_or_update_people(response_retweets, category="retweets")
                self.get_retweets_or_likes(response_retweets, category="retweets", article_dct=article_dct)
                time.sleep(random.randint(1, 3))
                # self.add_or_update_people(response_likes, category="likes")
                self.get_retweets_or_likes(response_likes, category="likes", article_dct=article_dct)
                time.sleep(random.randint(1, 3))

    def get_retweets_or_likes(self, dcts, category, article_dct):
        """获取转帖人/点赞人"""
        flag = 1 if category == "retweets" else 3
        article_url = article_dct["article_url"]

        peoples_dct = dcts["globalObjects"].get("users")
        if peoples_dct:
            for people_key in peoples_dct:
                people_dct = peoples_dct[people_key]
                follower_id = people_dct["id_str"]
                follower_nickname = people_dct["name"]
                follower_account = people_dct["screen_name"]

                conditions = f'{flag}-{article_url}-{follower_account}'  # 文章ID+关注/点赞人ID判断是否重复
                if self.bloom.exists(conditions):  # 如果已经存在
                    print('重复的社交关系：', conditions)
                    continue
                self.bloom.insert(conditions)  # 不存在就加入
                data = {
                    "wbParentId": article_dct.get("article_id"),  # 被评论帖子ID

                    "UId": article_dct.get("user_id"),  # 被评论者ID
                    "ScreenAccount": article_dct.get("Author"),  # 被评论者账号(传过来的账号)
                    "ScreenName": article_dct.get("AuthorNick"),  # 被评论者昵称(传过来的昵称)
                    "URL": "https://twitter.com/" + article_dct.get("account"),  # 被评论者主页URL

                    "FollowerUId": follower_id,  # 评论者id
                    "FollowerScreenName": follower_nickname,  # 评论者昵称
                    "FollowerAccount": follower_account,  # 评论者账号
                    "FollowerURL": "https://twitter.com/" + follower_account,  # 评论者主页URL

                    "PublishTime": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),  # 评论发布时间
                    "IsFriend": 0,  # 是否好友关系，0-是，1-否
                    # 数据来源  0：被关注；1：转发； 2：评论； 3：点赞；4：大爱；5：笑趴；6：哇；7：心碎；8：怒'
                    "Flag": flag,
                    "Platform": "twitter"  # 平台
                }
                relation_item = {
                    "DBAttributeValue": {
                        "DBKey": self.db_key_relations,
                        "DataType": 3
                    },
                    "ListSocialRelation": [data]
                }
                body = json.dumps(relation_item, ensure_ascii=False)
                self.channel.basic_publish(exchange='', routing_key=self.mq_relations_queue, body=body)
                print('ChromeDriver<-->社交关系推送成功：', body)


if __name__ == '__main__':
    twitter_obj = Twitter()
    twitter_obj.login()
    twitter_obj.start_crawl()
