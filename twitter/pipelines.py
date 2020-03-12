# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import json
import logging
import os
import re
import shutil
import time
from urllib.parse import urljoin

import pika
import requests
from lxml.html.clean import Cleaner
from scrapy.http import HtmlResponse

from twitter.bloom_filter import RedisClient, BloomFilter
from twitter.items import ArticleItem, CommentItem, SocialRelationItem


# class Xj13TwitterPipeline(object):
#     def process_item(self, item, spider):
#         return item


class MySQLPipeline(object):

    def __init__(self, mq_host, mq_username, mq_password, mq_port,
                 mq_articles_queue, mq_comments_queue, mq_relations_queue,
                 db_key, db_key_relations,
                 image_collect_enable, video_collect_enable,
                 redis_host, redis_port, redis_db, redis_key,
                 redis_key_urls, redis_key_urls_status
                 ):
        # MQ配置
        self.mq_host = mq_host
        self.mq_username = mq_username
        self.mq_password = mq_password
        self.mq_port = mq_port
        self.mq_articles_queue = mq_articles_queue
        self.mq_comments_queue = mq_comments_queue
        self.mq_relations_queue = mq_relations_queue
        # 项目配置db_key
        self.db_key = db_key
        self.db_key_relations = db_key_relations
        # 图片/视频下载配置
        self.image_collect_enable = image_collect_enable
        self.video_collect_enable = video_collect_enable
        # redis配置
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.redis_key = redis_key
        self.redis_key_urls = redis_key_urls
        self.redis_key_urls_status = redis_key_urls_status

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mq_host=crawler.settings.get("MQ_HOST"),
            mq_username=crawler.settings.get("MQ_USERNAME"),
            mq_password=crawler.settings.get("MQ_PASSWORD"),
            mq_port=crawler.settings.get("MQ_PORT"),
            mq_articles_queue=crawler.settings.get("MQ_QUEUE_ARTICLES"),
            mq_comments_queue=crawler.settings.get("MQ_QUEUE_COMMENTS"),
            mq_relations_queue=crawler.settings.get("MQ_QUEUE_RELATIONS"),

            db_key=crawler.settings.get("DB_KEY"),
            db_key_relations=crawler.settings.get("DB_KEY_RELATIONS"),

            image_collect_enable=crawler.settings.get("IMAGE_COLLECT_ENABLE"),
            video_collect_enable=crawler.settings.get("VIDEO_COLLECT_ENABLE"),

            redis_host=crawler.settings.get("REDIS_HOST"),
            redis_port=crawler.settings.get("REDIS_PORT"),
            redis_db=crawler.settings.get("REDIS_DB"),
            redis_key=crawler.settings.get("REDIS_KEY"),
            redis_key_urls=crawler.settings.get("REDIS_KEY_URLS"),
            redis_key_urls_status=crawler.settings.get("REDIS_KEY_URLS_STATUS")
        )

    def package_article_data(self, item):
        """帖子内容序列化"""
        data = {
            "DBAttributeValue": {
                "DataType": 0,
                "DBType_En": "Forum",
                "DBTypeName": "list",
                "IsSyncReturn": 0,
                "TempMqName": "",
                "DBKey": self.db_key,
                "ProcName": "Proc_App_InsertNews",
                "ProcParaName": "news",
                "ParaConfigName": "",
                "OptName": "",
                "OptTime": "",
                "Platform": 0
            },
            "ListNews": [
                {
                    "Platform": 0,
                    "PEID": 0,  # 新闻所属板块在本平台ID
                    "PRCID": "",  # 一些app需要该值作为请求参数跟评

                    "PNID": "id",  # 本平台内该新闻的唯一标识
                    "Url": "",  # 帖子url
                    "Author": "",  # 发帖人
                    "AuthorNick": "",  # 发帖人昵称
                    "PublishTime": "",  # 发帖时间
                    "Content": "",  # 内容

                    "CommentCount": "",  # 评论数
                    "ForwardNum": "",  # 转载数
                    "ClickCount": 0,  # 点击数
                    "LanguageCode": "",  # 语言编码

                    "Title": "",  # 帖子标题
                    "Keywords": "",  # 关键字
                    "Abstract": "",  # 摘要

                    "Category": "twitter",  # 频道英文标识
                    "CreateTime": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                    "ModifyTime": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),

                    "MediaSource": " ",
                    "MediaSourceUrl": "",
                    "GroupNumber": "",
                    "GroupName": ""
                },
            ],
            "ListComments": ""
        }
        data["ListNews"][0].update(item)
        return json.dumps(data, ensure_ascii=False)

    def package_comment_data(self, item):
        """评论序列化"""
        data = {
            "DBAttributeValue": {
                "DBKey": self.db_key,
                "DBTypeName": "评论",
                "DBType_En": "Comment",
                "DataType": 0,
                "IsSyncReturn": 0,
                "OptName": "",
                "OptTime": "",
                "ParaConfigName": "",
                "Platform": 0,
                "ProcName": "Proc_App_InsertComment",
                "ProcParaName": "comment",
                "TempMqName": ""
            },
            "ListComments": [
                {
                    "Platform": 0,
                    "PEID": 0,  # 新闻所属板块在本平台ID

                    "PNID": "",  # 帖子ID
                    "ParentPCID": "",  # 父PCID/帖子ID
                    "PCID": "",  # 评论的ID
                    "Author": "",  # 评论人账号
                    "AuthorNick": "",  # 评论人昵称
                    "AuthorID": "",  # 评论人ID
                    "Homepage": "",  # 评论人主页链接
                    "Content": "",  # 评论内容
                    "PublishTime": "",  # 评论发表时间
                    "AgreeCount": 0,  # 点赞数
                    "FollowCount": 0,  # 回复数
                    "LanguageCode": "",  # 语言编码
                    "Location": "",  # 评论人所在地

                    "Category": "twitter",  # 频道英文标识
                    "CreateTime": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())),
                    "ModifyTime": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))

                },
            ]
        }

        data["ListComments"][0].update(item)
        return json.dumps(data, ensure_ascii=False)

    def package_relations_data(self, item):
        data = {
            "DBAttributeValue": {
                "DBKey": self.db_key_relations,
                "DataType": 3
            },
            "ListSocialRelation": [
                {
                    "Platform": "twitter",  # 平台
                    "wbParentId": "",  # 被评论帖子ID

                    "UId": "",  # 被评论者ID
                    "ScreenName": "",  # 被评论者昵称
                    "ScreenAccount": "",  # 被评论者账号
                    "URL": "",  # 被评论者主页URL

                    "FollowerUId": "",  # 评论者id
                    "FollowerScreenName": "",  # 评论者昵称
                    "FollowerAccount": "",  # 评论者账号
                    "FollowerURL": "",  # 评论者主页URL

                    "PublishTime": "",  # 评论发布时间
                    "IsFriend": 0,  # 是否好友关系，0-是，1-否
                    "Flag": 2,  # 数据来源  0：被关注；1：转发； 2：评论； 3：点赞；4：大爱；5：笑趴；6：哇；7：心碎；8：怒'
                }
            ]
        }
        data["ListSocialRelation"][0].update(item)
        return json.dumps(data, ensure_ascii=False)

    def download_replace_images(self, article_url, content, image_urls, download_images_success):
        """下载内容中的图片并上传到资源服务器/并替换原来内容中的图片地址为资源服务器上的地址"""

        for old_url in image_urls:
            image_url = urljoin(article_url, old_url)  # 完整url
            new_url = self.fdfs_sender.download_upload_image(image_url=image_url)  # 下载并上传到资源服务器
            if new_url:  # 若下载/上传失败，则放弃(并标注下载失败)
                content = content.replace(old_url, new_url)
            else:
                download_images_success = False
        return download_images_success, content

    @staticmethod
    def download_video(article_id, article_url, video_url, max_failure_time=3):
        """下载内容中的视频,返回拼接后视频的二进制内容"""
        common_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.75 Safari/537.36', }
        session = requests.Session()
        session.headers.update(common_headers)

        # TODO 0.访问video地址
        res = session.get(url=video_url)
        response = HtmlResponse(url=video_url, body=res.text, encoding='utf-8')
        js_file_url = response.xpath("//script/@src").extract_first()

        # TODO 1.下载js文件，获取authorization
        for i in range(max_failure_time):
            try:
                js_file_content = session.get(js_file_url).content.decode()
                break
            except Exception as e:
                print(f'GET 下载f{js_file_url} --> 第{i+1}次下载失败, 失败原因：{e}')
        else:
            print(f'GET 下载f{js_file_url}已{max_failure_time}次全部下载失败 ---> {article_url}')
            return
        authorization = re.search('authorization:"(.*?)"', js_file_content, re.S)
        if authorization:
            header_authorization_field = authorization.group(1)
            headers = {
                'Host': 'api.twitter.com',
                'Connection': 'keep-alive',
                'Origin': '//twitter.com',
                'authorization': header_authorization_field,
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.75 Safari/537.36',
                'Accept': '*/*', 'Sec-Fetch-Site': 'same-site',
                'Referer': video_url,
                'Accept-Language': 'zh-CN,zh;q=0.9',
            }
            # 1.更新session请求头
            session.headers.update(headers)
            guest_token_address = 'https://api.twitter.com/1.1/guest/activate.json'

            # TODO 2.POST请求获取guest_token
            for i in range(max_failure_time):
                try:
                    guest_token_response = session.post(guest_token_address)
                    break
                except Exception as e:
                    print(f'POST 下载f{guest_token_address} --> 第{i+1}次下载失败, 失败原因：{e}')
            else:
                print(f'POST 下载f{guest_token_address}已{max_failure_time}次全部下载失败 ---> {article_url}')
                return
            if guest_token_response.status_code != 200:
                return
            guest_token = guest_token_response.json().get('guest_token')
            # 2.更新session请求头
            session.headers.update({'x-guest-token': guest_token})
            json_file_url = f'https://api.twitter.com/1.1/videos/tweet/config/{article_id}.json'

            # TODO 3.获取最外层json文件，里面含有(外层)m3u8地址
            for i in range(max_failure_time):
                try:
                    json_response_content = session.get(url=json_file_url).json()
                    break
                except Exception as e:
                    print(f'GET 下载f{json_file_url} --> 第{i+1}次下载失败, 失败原因：{e}')
            else:
                print(f'GET 下载f{json_file_url}已{max_failure_time}次全部下载失败 ---> {article_url}')
                return

            track = json_response_content.get('track')
            if not track:
                return
            m3u8_list_url = track.get('playbackUrl')

            # TODO 4.访问外层m3u8地址，里面含有(内层)m3u8地址
            for i in range(max_failure_time):
                try:
                    m3u8_list_content = requests.get(m3u8_list_url).text
                    break
                except Exception as e:
                    print(f'GET 下载f{m3u8_list_url} --> 第{i+1}次下载失败, 失败原因：{e}')
            else:
                print(f'GET 下载f{m3u8_list_url}已{max_failure_time}次全部下载失败 ---> {article_url}')
                return

            if "#EXTM3U" not in m3u8_list_content:
                # raise BaseException("非M3U8的链接")
                return
            if "EXT-X-STREAM-INF" in m3u8_list_content:  # 第一层
                file_line = m3u8_list_content.split("\n")
                for line in file_line:
                    if '.m3u8' in line:  # 注：只需要第一个即可
                        m3u8_file_url = urljoin(m3u8_list_url, line)  # 拼出第二层m3u8的URL
                        break
                else:  # 没有找到内层M3U8链接
                    return

                # TODO 5.访问内层m3u8地址，里面含有.ts文件
                for i in range(max_failure_time):
                    try:
                        content = requests.get(m3u8_file_url).content.decode()
                        break
                    except Exception as e:
                        print(f'GET 下载f{m3u8_file_url} --> 第{i+1}次下载失败, 失败原因：{e}')
                else:
                    print(f'GET 下载f{m3u8_file_url}已{max_failure_time}次全部下载失败 ---> {article_url}')
                    return

                file_line = content.split('\n')
                video_content = bytes()
                for index, line in enumerate(file_line):
                    if "EXTINF" in line:  # 找ts地址并下载
                        # TODO 6.筛选出.ts文件并下载
                        ts_file_url = urljoin(m3u8_file_url, file_line[index + 1])  # 拼出ts片段的URL
                        for i in range(max_failure_time):
                            try:
                                ts_content = requests.get(ts_file_url, headers=common_headers).content
                                video_content += ts_content
                                break
                            except Exception as e:
                                # 第i次下载失败
                                print(f'GET 下载f{ts_file_url} --> 第{i+1}次下载失败, 失败原因：{e}')
                        else:
                            # 全部下载失败
                            print(f'GET 下载f{ts_file_url}已{max_failure_time}次全部下载失败 ---> {article_url}')
                            return
                return video_content

    def process_item(self, item, spider):
        if isinstance(item, ArticleItem):
            # 推帖子
            article_id = item['PNID']
            article_url = item['Url']
            content = item['Content']
            content = content.replace("amp;", "").replace('data-src="', 'src="')
            # 需要POP的参数
            video_url = item.pop('VideoUrl')
            is_new = item.pop('is_new')
            redis_data = {
                "article_id": article_id,
                "article_url": article_url,
                # 假/伪/真
                "Author": item.get("Author"),
                "AuthorNick": item.get("AuthorNick"),
                "user_id": item.pop("user_id"),
                "account": item.pop("account")
            }

            if self.image_collect_enable or self.video_collect_enable:  # 是否需要下载图片/视频
                # 默认图片/视频下载成功
                download_images_success = True
                download_videos_success = True
                response = HtmlResponse(url=article_url, body=content, encoding='utf-8')
                image_urls = response.xpath("//img/@src").extract()
                if image_urls or video_url:  # 是否有图片/视频
                    if not self.bloom.exists(article_url):  # 判断是否在布隆过滤器里面，在里面放弃操作
                        # 1、分开为两个try: 只要图片或内容有一种下载成功，第二次将不再下载，
                        # 2、合并为一个try: 必须图片和视频都要下载成功，第二次才不再下载
                        try:
                            # 是否下载图片，且有图片的情况下
                            if self.image_collect_enable and image_urls:
                                download_images_success, content = self.download_replace_images(
                                    article_url, content, image_urls, download_images_success)
                        except:
                            pass

                        try:
                            # 是否下载视频，且有视频的情况下
                            if self.video_collect_enable and video_url:
                                video_content = self.download_video(article_id, article_url, video_url)
                                if video_content:  # 下载视频流成功
                                    ffmpeg = r'ffmpeg.exe'
                                    video_dir_path = "./videos"
                                    file_ts = f'{video_dir_path}/{article_id}.ts'
                                    file_mp4 = f'{video_dir_path}/{article_id}.mp4'

                                    # 1.存成.ts文件
                                    f1 = open(file_ts, 'wb')
                                    f1.write(video_content)
                                    f1.close()

                                    # 2.执行命令，转成mp4文件
                                    cmd = ffmpeg + " -i " + file_ts + " -c copy " + file_mp4
                                    os.system(cmd)

                                    # 3.读mp4
                                    f3 = open(file_mp4, 'rb')
                                    video_content_upload = f3.read()
                                    f3.close()

                                    # 4.上传文件服务器
                                    video_resource_address = self.fdfs_sender.send_video(video_content_upload)

                                    # 5.删除ts/mp4文件
                                    try:
                                        os.remove(file_ts)
                                        os.remove(file_mp4)
                                    except Exception as e:
                                        logging.error(e)
                                        if os.path.exists(video_dir_path):
                                            shutil.rmtree(video_dir_path)
                                        if not os.path.exists(video_dir_path):
                                            os.mkdir(video_dir_path)

                                    if video_resource_address:  # 若上传失败，则放弃(并标注下载失败)
                                        response = HtmlResponse(url=article_url, body=content, encoding='utf-8')
                                        video_urls = response.xpath("//video/@src").extract()
                                        for old_url in video_urls:
                                            content = content.replace(old_url, video_resource_address)
                                    else:
                                        download_videos_success = False
                                    tag_video = f'<video controls autoplay src="{video_resource_address}"></video>'
                                    content += tag_video
                        except:
                            pass

                    # 既有图片又有视频，且都下载成功
                    if image_urls and video_url:
                        if download_images_success and download_videos_success:
                            self.bloom.insert(article_url)
                    # 只有图片或视频的一种情况
                    elif image_urls or video_url:
                        if image_urls and download_images_success:
                            self.bloom.insert(article_url)
                        elif video_url and download_videos_success:
                            self.bloom.insert(article_url)

            content = self.cleaner.clean_html(content).replace('<div>', '').replace('</div>', '')
            item['Content'] = content
            body = self.package_article_data(item)
            self.channel.basic_publish(exchange='', routing_key=self.mq_articles_queue, body=body)
            print('帖子推送MQ成功--->', body)
            log_data = {
                "url": article_url,
                "author": item["Author"],
                "publish_time": item["PublishTime"],
                "MediaSourceUrl": item["MediaSourceUrl"]
            }
            logging.warning(log_data)
            if self.push_relations_to_redis or (is_new and self.open_new_procedure):
                self.redis_client.push(self.redis_key_urls, json.dumps(redis_data, ensure_ascii=False))

        elif isinstance(item, CommentItem):
            # 推评论
            content = item['Content']
            content = self.cleaner.clean_html(content).replace('<div>', '').replace('</div>', '')
            item['Content'] = content
            body = self.package_comment_data(item)
            self.channel.basic_publish(exchange='', routing_key=self.mq_comments_queue, body=body)
            print('评论推送MQ成功--->', body)

        elif isinstance(item, SocialRelationItem):
            # 推社交关系
            body = self.package_relations_data(item)
            self.channel.basic_publish(exchange='', routing_key=self.mq_relations_queue, body=body)
            print('社交关系推送MQ成功--->', body)

        return item

    def open_spider(self, spider):
        """连接MQ
        :param spider:
        :return:
        """
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.mq_host, port=self.mq_port,
                                      credentials=pika.PlainCredentials(self.mq_username, self.mq_password),
                                      heartbeat=0))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.mq_articles_queue, durable=False)  # 推特帖子队列
        self.channel.queue_declare(queue=self.mq_comments_queue, durable=False)  # 评论队列
        self.channel.queue_declare(queue=self.mq_relations_queue, durable=False)  # 社交关系队列

        # 需要保留的标签
        allow_tags = ['p', 'br', 'img', 'video']
        # 需要保留的属性
        allow_attrs = ['src', 'controls']
        self.cleaner = Cleaner(style=True, scripts=True, comments=True, javascript=True,
                               page_structure=True, safe_attrs_only=True, remove_unknown_tags=False,
                               safe_attrs=frozenset(allow_attrs), allow_tags=allow_tags)
        # 连接redis
        self.redis_client = RedisClient(host=self.redis_host, port=self.redis_port, redis_db=self.redis_db)
        # 社交关系通道是否存活
        status = spider.status
        if status:  # 如果存活就不再推送至redis
            self.push_relations_to_redis = 0
        else:  # 不存在则再新建一个存活时间为1天的key
            self.push_relations_to_redis = 1
            self.redis_client.db.set(self.redis_key_urls_status, 1, ex=24 * 60 * 60)
        # 是否开启新的进程/线程，并推送？
        self.open_new_procedure = spider.open_new_procedure

        # 若下载图片或者视频则启用布隆过滤器
        if self.image_collect_enable or self.video_collect_enable:
            from twitter.fdfs_send import Sender
            self.bloom = BloomFilter(server=self.redis_client, bloom_key=self.redis_key)
            self.fdfs_sender = Sender()

    def close_spider(self, spider):
        """关闭MQ
        :param spider:
        :return:
        """
        if self.push_relations_to_redis or (spider.new_user_count and self.open_new_procedure):
            redis_data = {
                "is_end": 1,
                "article_id": "",
                "article_url": "",
                "origin_author_id": "",  # 源作者ID
                "origin_author": "",  # 源作者账户
                "origin_author_nickname": ""  # 源作者昵称
            }
            self.redis_client.push(self.redis_key_urls, json.dumps(redis_data, ensure_ascii=False))
        if self.connection.is_open:
            self.connection.close()  # 断开MQ连接
        if self.image_collect_enable or self.video_collect_enable:
            self.redis_client.close()  # 断开redis连接
        pass
