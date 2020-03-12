# -*- coding: utf-8 -*-

import json
import os
import re
import datetime
import copy
import time
from multiprocessing import Process
from threading import Thread

import requests
import scrapy
from scrapy.http import HtmlResponse

from twitter.items import ArticleItem, CommentItem, SocialRelationItem, ArticleCount
from twitter.settings import USER_ACCOUNTS_URL, PlatformName, ProjectName, USER_MSG_CHOOSE
from twitter.twitter_relations import Twitter


class SearchSpider(scrapy.Spider):
    name = 'twitter_spider'

    account_urls = USER_ACCOUNTS_URL
    PlatformName = PlatformName
    ProjectName = ProjectName

    # 首次爬取
    end_time = datetime.datetime.now() - datetime.timedelta(days=7)
    e_time = int(end_time.timestamp())

    # 增量爬取
    inc_end_time = datetime.datetime.now() - datetime.timedelta(days=3)
    inc_e_time = int(inc_end_time.timestamp())

    # 增量爬取评论筛选时间
    comment_time_interval = 1
    # 时间限制时，若帖子数量达不到要求则进行数量限制
    article_count_limit = 100
    # 是否有新用户进来
    new_user_count = 0
    # 是否开启线程/进程消费消费redis中新增账号的社交关系
    open_new_procedure = False
    # 用户信息选择
    user_msg_choose = USER_MSG_CHOOSE
    max_failure_time = 3
    common_headers = {
        'Host': 'twitter.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.75 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
        'Accept-Language': 'zh-CN,zh;q=0.9'
    }

    def __init__(self, name=name, **kwargs):
        super().__init__(name=name, **kwargs)
        self.increment_crawl = int(kwargs.get('is_increment_crawl'))
        self.status = int(kwargs.get('status'))

    def start_requests(self):
        user_accounts_path = "./user_accounts.json"
        if not os.path.exists(user_accounts_path):
            with open(user_accounts_path, 'w', encoding='utf-8') as f:
                data = {"accounts": []}
                json.dump(data, f, ensure_ascii=False)

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
            "PlatformName": self.PlatformName,
            "ProjectName": self.ProjectName
        }

        response = requests.post(url=self.account_urls, data=json.dumps(body), headers=headers, verify=False)
        task = json.loads(response.text)
        user_list = task.get('ListTask')

        # user_list = [
        #     {'Url': 'https://twitter.com/yuceltanay53', 'UserAccount': 'yuceltanay53', 'ScreenName': 'yücel  tanay'}]

        # 加载本地账号信息
        with open(user_accounts_path, 'r+', encoding='utf-8') as fb:
            local_user_accounts = json.load(fb).get("accounts")
            local_user_accounts_list = []
            if local_user_accounts:
                for user in local_user_accounts:
                    local_user_accounts_list.append(user.get("Url"))
            # 更新本地账号信息
            fb.seek(0)
            fb.truncate()
            data = {"accounts": user_list}
            json.dump(data, fb, ensure_ascii=False)

        if user_list:
            for user in user_list:
                user_account = user.get("Url")
                if user_account not in local_user_accounts_list:
                    # 表示为新增的账号
                    user["is_new"] = True
                    self.new_user_count += 1
                    user_list.remove(user)  # 移除新增的账号
                    user_list.insert(0, user)  # 添加新增的账号到最开头

            # key未过期，且又有新账号来临时，开启新的线程/进程消费社交关系
            if self.status and self.new_user_count and self.open_new_procedure:
                # 开启新的线程消费新增账号的社交关系
                # Thread(target=self.start_consume_relations, args=(0,)).start()
                # 开启新的进程消费新增账号的社交关系d
                Process(target=self.start_consume_relations, args=(0,)).start()

            # 然后再慢慢遍历,看是否有新的账号进来
            for user in user_list:
                user_account = user.get('UserAccount')
                user_nick = user.get('ScreenName')
                language_code = user.get('LanguageCode')
                is_new = user.get('is_new')
                url = user.get('Url')
                # article_count = ArticleCount(Url=url, count=0)
                yield scrapy.Request(url=url, callback=self.parse_user_page,
                                     meta={"XXX_UserAccount": user_account,
                                           "XXX_AuthorNick": user_nick,
                                           "LanguageCode": language_code,
                                           # "is_new": is_new,
                                           # "article_count": article_count,
                                           })

    # 解析用户首页
    def parse_user_page(self, response):
        """解析推特人物首页(获取帖子Url)"""
        article_count = response.meta.get("article_count")
        # 终止翻页
        # if article_count.get("end_collect"):
        #     return []

        # 用于翻页/传递社交关系
        user_id = response.xpath(
            "//li[contains(@class,'userActions')]//div[contains(@class,'user-actions')]/@data-user-id").extract_first()
        user_account = response.xpath(
            "//li[contains(@class,'userActions')]//div[contains(@class,'user-actions')]/@data-screen-name").extract_first()
        author_nick = response.xpath(
            "//li[contains(@class,'userActions')]//div[contains(@class,'user-actions')]/@data-name").extract_first()

        # 伪.传值(current_page、article_count、UserAccount、AuthorNick与人一一 对应，
        # 但是文章真实 账号ID、账号、作者与文章一一对应)
        response.meta2 = response.meta
        response.meta2["user_id"] = user_id
        response.meta2["UserAccount"] = user_account
        response.meta2["AuthorNick"] = author_nick
        current_page = 1
        article_requests = self.make_article_request(response, current_page=current_page)
        for articleRequest in article_requests:
            yield articleRequest

        # 第二页帖子的地址
        next_page = response.xpath("//div[@data-min-position]/@data-min-position").extract_first()
        if next_page:
            next_url = f'https://twitter.com/i/profiles/show/{user_account}/timeline/tweets?include_available_features=1&include_entities=1&max_position={next_page}&reset_error_state=false'
            meta = response.meta2
            meta['current_page'] = current_page + 1
            yield scrapy.Request(url=next_url, callback=self.parse_other_list_page, meta=meta)

    # 用户首页往下翻页
    def parse_other_list_page(self, response):
        """解析其他帖子列表页"""
        article_count = response.meta.get("article_count")
        # 终止翻页
        if article_count.get("end_collect"):
            return []

        # 用于翻页
        user_account = response.meta.get("UserAccount")
        current_page = response.meta.get('current_page')
        # if current_page > 5:  # 固定只采5页(100篇)
        #     return []
        try:
            res = json.loads(response.text)
        except:
            return []
        article_list_response = HtmlResponse(url=response.url, body=res.get('items_html'), encoding='utf-8')
        article_list_response.meta2 = response.meta
        article_requests = self.make_article_request(article_list_response, current_page=current_page)
        for articleRequest in article_requests:
            yield articleRequest

        # 第三页及以后的帖子列表页
        has_next = res.get('has_more_items') and res.get('min_position')
        if has_next:
            next_page = res.get('min_position')
            next_url = f'https://twitter.com/i/profiles/show/{user_account}/timeline/tweets?include_available_features=1&include_entities=1&max_position={next_page}&reset_error_state=false'
            meta = response.meta
            meta['current_page'] = current_page + 1
            yield scrapy.Request(url=next_url, callback=self.parse_other_list_page, meta=meta)

    # 解析帖子列表页(首页、翻页)、构造出帖子scrapy.Request对象(因为翻页是json格式的，与首页分开写就重复了)
    def make_article_request(self, response, current_page):
        """解析、构造出帖子scrapy.Request对象(因为用户首页是Html,翻页是json，所以提取出来)"""
        article_count = response.meta2.get("article_count")

        article_selectors = response.xpath("//li[@data-item-id][@data-item-type='tweet']")
        for article in article_selectors:
            article_url = article.xpath(".//small[@class='time']/a/@href").extract_first()
            if article_count.get("end_collect"):
                return []
            meta = response.meta2
            meta["current_page"] = current_page
            yield scrapy.Request(url=response.urljoin(article_url), callback=self.parse_article_detail, meta=meta)

        # meta = response.meta2
        # meta['publish_time'] = "------"
        # url = "https://twitter.com/VOAChinese/status/1215086297246646273"
        # yield scrapy.Request(url=response.urljoin(url), callback=self.parse_article_detail, meta=meta)

    # 解析帖子iframe标签里面的内容
    def parse_related_article(self, url):
        """相关文章(图片)
        # url = 'https://twitter.com/i/cards/tfw/v1/%s?cardname=summary_large_image&autoplay_disabled=true&earned=true&edge=true&lang=zh-cn&card_height=344&scribe_context={"client":"web","page":"permalink","section":"permalink","component":"tweet"}' % (t_id)
        # url = f'https://twitter.com/i/cards/tfw/v1/{t_id}?cardname=summary_large_image&autoplay_disabled=true&earned=true&edge=true&lang=zh-cn&card_height=344&scribe_context=%7B%22client%22%3A%22web%22%2C%22page%22%3A%22permalink%22%2C%22section%22%3A%22permalink%22%2C%22component%22%3A%22tweet%22%7D'
        """
        res = self.requests_of_get(url=url)
        if not res:
            return ''
        response = HtmlResponse(url=url, body=res.content.decode(), encoding='utf-8')
        content = response.xpath("//div[contains(@class,'CardContent')]").extract_first()
        return content

    # 解析帖子详情页
    def parse_article_detail(self, response):
        """解析推特帖子详情页 以及首页评论"""
        current_page = response.meta.get("current_page")
        article_count = response.meta.get("article_count")

        is_new = response.meta.get("is_new")
        user_id = response.meta.get("user_id")
        user_account = response.meta.get("UserAccount")  # 帖子作者(不一定是帖子原作者，而是所查询的用户)
        user_nick = response.meta.get("AuthorNick")  # 帖子作者昵称(不一定是帖子原作者，而是所查询的用户)
        XXX_UserAccount = response.meta.get("XXX_UserAccount")
        XXX_AuthorNick = response.meta.get("XXX_AuthorNick")

        # 发布时间
        xpath_publish_time = "//div[contains(@class,'permalink-inner permalink-tweet-container')]//small[@class='time']//span[@data-time]/@data-time"
        ptime = response.xpath(xpath_publish_time).extract_first()
        if not ptime:
            return []
        p_time = int(ptime.strip())
        publish_time = str(datetime.datetime.fromtimestamp(p_time))
        if current_page > 1:  # 每个账号必采1页
            if not self.increment_crawl or is_new:  # 首次采集/新增账号
                if p_time < self.e_time and article_count["count"] >= self.article_count_limit:
                    article_count["end_collect"] = True
                    return []
            else:  # 增量爬取/且不是新增账号，大于第1页的采(self.inc_e_time)天以内
                if p_time < self.inc_e_time:
                    article_count["end_collect"] = True
                    return []

        # 继续采集 +1
        article_count["count"] += 1

        # 帖子url
        article_url = response.url
        # 文章ID
        # article_id = url.split('/')[-1]
        xpath_article_id = "//div[contains(@class,'permalink-inner permalink-tweet-container')]//div[contains(@class,'js-actionable-user')]/@data-item-id"
        article_id = response.xpath(xpath_article_id).extract_first()

        # (原作者)作者账号 (作用：1.下一页拼接  2.社交关系)
        xpath_origin_author = "//div[contains(@class,'permalink-inner permalink-tweet-container')]//div[contains(@class,'js-actionable-user')]/@data-screen-name"
        origin_author = response.xpath(xpath_origin_author).extract_first()

        # (原作者)作者昵称
        xpath_origin_author_nick = "//div[contains(@class,'permalink-inner permalink-tweet-container')]//div[contains(@class,'js-actionable-user')]/@data-name"
        origin_author_nickname = response.xpath(xpath_origin_author_nick).extract_first()

        # (原作者)作者ID
        xpath_origin_author_id = "//div[contains(@class,'permalink-inner permalink-tweet-container')]//div[contains(@class,'js-actionable-user')]/@data-user-id"
        origin_author_id = response.xpath(xpath_origin_author_id).extract_first()

        # 帖子内容(1.文本内容  2.视频内容  3.(未采集)其他底部视频内容(包含时间、转推、点赞等)[js-tweet-details-fixer tweet-details-fixer])
        xpath_content = "//div[contains(@class,'permalink-inner permalink-tweet-container')]//div[@class='js-tweet-text-container'] | //div[contains(@class,'permalink-inner permalink-tweet-container')]//div[@class='AdaptiveMediaOuterContainer'] | //div[contains(@class,'permalink-inner permalink-tweet-container')]//div[contains(@class,'u-block js-tweet-details-fixer')]"
        content = ''.join(response.xpath(xpath_content).extract())

        # 帖子内容语言代码
        xpath_content_language_code = "//div[contains(@class,'permalink-inner permalink-tweet-container')]//p/@lang"
        content_language_code = response.xpath(xpath_content_language_code).extract_first()

        # 评论数
        xpath_comment = "//div[contains(@class,'permalink-inner permalink-tweet-container')]//div[contains(@class,'stream-item-footer')]//div[contains(@class,'ProfileTweet-actionCountList')]//span[contains(@class,'ProfileTweet-action--reply')]/span[@class='ProfileTweet-actionCount']/@data-tweet-stat-count"
        comments = response.xpath(xpath_comment).extract_first()
        comments_count = int(comments.strip()) if comments else 0

        # 转推数
        xpath_retweets = "//div[contains(@class,'permalink-inner permalink-tweet-container')]//div[contains(@class,'stream-item-footer')]//div[contains(@class,'ProfileTweet-actionCountList')]//span[contains(@class,'ProfileTweet-action--retweet')]/span[@class='ProfileTweet-actionCount']/@data-tweet-stat-count"
        retweets = response.xpath(xpath_retweets).extract_first()
        retweets_count = int(retweets) if retweets else 0

        # 点赞数
        xpath_likes = "//div[contains(@class,'permalink-inner permalink-tweet-container')]//div[contains(@class,'stream-item-footer')]//div[contains(@class,'ProfileTweet-actionCountList')]//span[contains(@class,'ProfileTweet-action--favorite')]/span[@class='ProfileTweet-actionCount']/@data-tweet-stat-count"
        likes = response.xpath(xpath_likes).extract_first()
        likes_count = int(likes) if likes else 0

        # 帖子内容下面部分的iframe标签里面的html
        xpath_additional_url = "//div[contains(@class,'permalink-inner permalink-tweet-container')]//div[contains(@class,'js-macaw-cards-iframe-container')]/@data-full-card-iframe-url"
        additional_url = response.xpath(xpath_additional_url).extract_first()
        additional_content = self.parse_related_article(url=response.urljoin(additional_url))
        content = content + additional_content if additional_content else content

        # 视频地址
        video_url = response.xpath("//meta[@property='og:video:secure_url']/@content").extract_first()

        if not content:  # 丢弃没有内容的文章
            return

        if self.user_msg_choose == 1:  # 假
            user_id_choose = user_id
            account = user_account
            user_account_choose = XXX_UserAccount
            user_nick_choose = XXX_AuthorNick
        elif self.user_msg_choose == 2:  # 伪
            user_id_choose = user_id
            account = user_account
            user_account_choose = user_account
            user_nick_choose = user_nick
        else:  # 真
            user_id_choose = origin_author_id
            account = origin_author
            user_account_choose = origin_author
            user_nick_choose = origin_author_nickname

        article_data = {
            "PNID": article_id,
            "Url": article_url,
            "Author": user_account_choose,  # 传过来的账号
            "AuthorNick": user_nick_choose,  # 传过来的昵称
            "PublishTime": publish_time,
            "Content": content,
            "CommentCount": comments_count,  # 评论数
            "ForwardNum": retweets_count,  # 转载数
            "ClickCount": likes_count,  # 点击数(点赞数？)
            "LanguageCode": content_language_code or 'en',  # 语言编码 response.meta.get("LanguageCode")

            "Title": "",
            "Abstract": "",
            "Keywords": "",

            "VideoUrl": video_url,
            "MediaSourceUrl": video_url or "",

            "is_new": is_new,
            "user_id": user_id_choose,
            "account": account
        }
        article_item = ArticleItem(**article_data)
        yield article_item

        """解析帖子页-->首页评论"""
        current_comment_page = 1
        response.meta2 = copy.deepcopy(response.meta)
        response.meta2['article_id'] = article_id
        response.meta2['article_url'] = article_url
        # 假/伪/真
        response.meta2['user_id_choose'] = user_id_choose
        response.meta2['user_account_choose'] = user_account_choose
        response.meta2['user_nick_choose'] = user_nick_choose
        response.meta2['account'] = account

        response.meta2['current_comment_page'] = current_comment_page  # 一级评论的页数

        """解析评论"""
        comments = self.parse_comment(response)
        for comment in comments:  # 创建评论对象，同时更新社交关系
            if isinstance(comment, scrapy.Request):
                yield comment
            else:
                if comment.pop("is_end_comment"):
                    return []
                # 被评论人等信息
                data_reply_to_users = comment.pop("data_reply_to_users")
                new_relations_dct = comment.pop("NEW_RELATIONS")
                comment_item = CommentItem(**comment)  # pop后就创建字典
                yield comment_item
                # social_relations = self.make_social_relation_dict(data_reply_to_users, comment)
                # for relation in social_relations:
                #     relation_item = SocialRelationItem(ListSocialRelation=[])
                #     relation_item["ListSocialRelation"].append(relation)
                #     yield relation_item

                social_relation = self.make_social_relation_dict_bad(new_relations_dct, comment)
                relation_item = SocialRelationItem(**social_relation)
                yield relation_item

        # 下一页评论地址(第二页)
        xpath_next_page = "//div[@class='ThreadedDescendants']/div[contains(@class,'stream-container')]/@data-min-position"
        has_next_page = response.xpath(xpath_next_page).extract_first()
        # 显示更多回复
        xpath_more_comment = "//li[@class='ThreadedConversation-showMoreThreads']/button/@data-cursor"
        has_more_comment = response.xpath(xpath_more_comment).extract_first()
        has_next = has_next_page or has_more_comment
        if has_next:
            next_url = 'https://twitter.com/i/' + origin_author + '/conversation/' + article_id + '?include_available_features=1&include_entities=1&max_position=' + has_next
            meta = copy.deepcopy(response.meta2)
            meta["commentator_account"] = origin_author
            meta["comment_id"] = article_id
            meta["current_comment_page"] = current_comment_page + 1  # 第二页的评论
            yield scrapy.Request(url=next_url, callback=self.parse_other_comment_page, meta=meta)

    # 解析其他页一级评论页
    def parse_other_comment_page(self, response):
        """解析其他页一级评论页(json格式)"""
        # 翻页做准备
        commentator_account = response.meta.get("commentator_account")
        comment_id = response.meta.get("comment_id")
        # 评论翻页计数
        current_comment_page = response.meta.get("current_comment_page")
        is_new = response.meta.get("is_new")
        # if self.increment_crawl and not is_new and current_comment_page >= 3:
        #     return []
        try:
            res = json.loads(response.text)
        except:
            return []
        comment_list_response = HtmlResponse(url=response.url, body=res.get('items_html'), encoding='utf-8')
        comment_list_response.meta2 = copy.deepcopy(response.meta)
        comments = self.parse_comment(comment_list_response)
        for comment in comments:  # 创建评论对象，同时更新社交关系
            if isinstance(comment, scrapy.Request):
                yield comment
            else:
                if comment.pop("is_end_comment"):  # 增量爬取，终止继续采集评论
                    return []
                # 被评论人等信息
                data_reply_to_users = comment.pop("data_reply_to_users")
                new_relations_dct = comment.pop("NEW_RELATIONS")
                comment_item = CommentItem(**comment)  # pop后就创建字典
                yield comment_item
                # social_relations = self.make_social_relation_dict(data_reply_to_users, comment)
                # for relation in social_relations:
                #     relation_item = SocialRelationItem(ListSocialRelation=[])
                #     relation_item["ListSocialRelation"].append(relation)
                #     yield relation_item

                social_relation = self.make_social_relation_dict_bad(new_relations_dct, comment)
                relation_item = SocialRelationItem(**social_relation)
                yield relation_item

        # 其他一级评论页 再翻页
        has_more_comment = self.has_more_comment(comment_list_response)
        has_next = res.get('min_position') or has_more_comment
        if has_next:  # 是否含有下一页
            next_url = 'https://twitter.com/i/' + commentator_account + '/conversation/' + comment_id + '?include_available_features=1&include_entities=1&max_position=' + has_next
            if len(next_url) >= 10000:
                return []
            else:
                meta = copy.deepcopy(response.meta)
                meta["current_comment_page"] = current_comment_page + 1
                yield scrapy.Request(url=next_url, callback=self.parse_other_comment_page, meta=meta)

    # 解析二级评论首页
    def parse_secondary_comment_page(self, response):
        """解析回复数大于1的评论首页 Html格式
           得到二级评论
        """
        # 翻页做准备
        commentator_account = response.meta.get("commentator_account")
        comment_id = response.meta.get("comment_id")

        current_secondary_comment_page = 1
        response.meta2 = copy.deepcopy(response.meta)
        response.meta2["current_secondary_comment_page"] = current_secondary_comment_page
        comments = self.parse_secondary_comment(response)  # 解析二级评论
        for comment in comments:  # 创建评论对象，同时更新社交关系
            if isinstance(comment, scrapy.Request):
                yield comment
            else:
                if comment.pop("is_end_comment"):
                    return []
                # 被评论人等信息
                data_reply_to_users = comment.pop("data_reply_to_users")
                new_relations_dct = comment.pop("NEW_RELATIONS")
                comment_item = CommentItem(**comment)  # pop后就创建字典
                yield comment_item
                # social_relations = self.make_social_relation_dict(data_reply_to_users, comment)
                # for relation in social_relations:
                #     relation_item = SocialRelationItem(ListSocialRelation=[])
                #     relation_item["ListSocialRelation"].append(relation)
                #     yield relation_item

                social_relation = self.make_social_relation_dict_bad(new_relations_dct, comment)
                relation_item = SocialRelationItem(**social_relation)
                yield relation_item

        """二级评论首页翻页 """
        # 下一页评论地址(第二页)
        xpath_next_page = "//div[@class='ThreadedDescendants']/div[contains(@class,'stream-container')]/@data-min-position"
        has_next_page = response.xpath(xpath_next_page).extract_first()
        # 显示更多回复
        xpath_more_comment = "//li[@class='ThreadedConversation-showMoreThreads']/button/@data-cursor"
        has_more_comment = response.xpath(xpath_more_comment).extract_first()
        has_next = has_next_page or has_more_comment
        if has_next:  # 情况1,还有评论就继续传递更新社交关系, 没有更多二级评论就不用管
            next_url = 'https://twitter.com/i/' + commentator_account + '/conversation/' + comment_id + '?include_available_features=1&include_entities=1&max_position=' + has_next
            meta = copy.deepcopy(response.meta)
            meta["current_secondary_comment_page"] = current_secondary_comment_page + 1
            yield scrapy.Request(url=next_url, callback=self.parse_other_secondary_comment_page, meta=meta)

    # 解析其他二级评论页
    def parse_other_secondary_comment_page(self, response):
        """解析其他二级评论页(json格式)"""
        # 翻页做准备
        commentator_account = response.meta.get("commentator_account")
        comment_id = response.meta.get("comment_id")
        current_secondary_comment_page = response.meta.get("current_secondary_comment_page")
        try:
            res = json.loads(response.text)
        except:
            return []
        comment_list2_response = HtmlResponse(url=response.url, body=res.get('items_html'), encoding='utf-8')
        comment_list2_response.meta2 = copy.deepcopy(response.meta)
        comments = self.parse_secondary_comment(comment_list2_response)
        for comment in comments:
            if isinstance(comment, scrapy.Request):
                yield comment
            else:
                if comment.pop("is_end_comment"):
                    return []
                # 被评论人等信息
                data_reply_to_users = comment.pop("data_reply_to_users")
                new_relations_dct = comment.pop("NEW_RELATIONS")
                comment_item = CommentItem(**comment)  # pop后就创建字典
                yield comment_item
                # social_relations = self.make_social_relation_dict(data_reply_to_users, comment)
                # for relation in social_relations:
                #     relation_item = SocialRelationItem(ListSocialRelation=[])
                #     relation_item["ListSocialRelation"].append(relation)
                #     yield relation_item

                social_relation = self.make_social_relation_dict_bad(new_relations_dct, comment)
                relation_item = SocialRelationItem(**social_relation)
                yield relation_item

        """其他二级评论页 再翻页"""
        has_more_comment = self.has_more_comment(comment_list2_response)
        has_next = res.get('min_position') or has_more_comment
        if has_next:  # 是否含有下一页
            next_url = 'https://twitter.com/i/' + commentator_account + '/conversation/' + comment_id + '?include_available_features=1&include_entities=1&max_position=' + has_next
            meta = copy.deepcopy(response.meta)
            meta["current_secondary_comment_page"] = current_secondary_comment_page + 1
            yield scrapy.Request(url=next_url, callback=self.parse_other_secondary_comment_page, meta=meta)

    # 解析其他二级评论块--另外？条回复
    def parse_show_more_more_replies(self, response):
        """每个二级评论块可能有--另外？条回复"""
        try:
            res = json.loads(response.text)
        except:
            return []
        comment_list3_response = HtmlResponse(url=response.url, body=res.get('conversation_html'), encoding='utf-8')
        comment_list3_response.meta2 = copy.deepcopy(response.meta)
        comments = self.parse_secondary_comment(comment_list3_response)
        for comment in comments:  # 创建评论对象，同时更新社交关系
            if isinstance(comment, scrapy.Request):
                yield comment
            else:
                comment.pop("is_end_comment")
                # 被评论人等信息
                data_reply_to_users = comment.pop("data_reply_to_users")
                new_relations_dct = comment.pop("NEW_RELATIONS")
                comment_item = CommentItem(**comment)  # pop后就创建字典
                yield comment_item
                # social_relations = self.make_social_relation_dict(data_reply_to_users, comment)
                # for relation in social_relations:
                #     relation_item = SocialRelationItem(ListSocialRelation=[])
                #     relation_item["ListSocialRelation"].append(relation)
                #     yield relation_item

                social_relation = self.make_social_relation_dict_bad(new_relations_dct, comment)
                relation_item = SocialRelationItem(**social_relation)
                yield relation_item

    # 一级评论|二级评论，翻页(显示更多回复)
    @staticmethod
    def has_more_comment(response):
        """一级评论|二级评论，翻页(显示更多回复)"""
        xpath_next = "//li[@class='ThreadedConversation-showMoreThreads']/button/@data-cursor"
        next_address = response.xpath(xpath_next).extract_first()
        return next_address

    # 构造社交关系(评论)字典
    @staticmethod
    def make_social_relation_dict(data_reply_to_users, comment):
        """构造social_relation字典
        :param data_reply_to_users: 被评论人等信息
        :param comment: CommentItem
        :return:social_relation字典
        """
        for data_reply in data_reply_to_users:
            social_relation = {
                "Platform": "twitter",  # 平台
                "wbParentId": comment.get("PNID"),  # 被评论帖子ID

                "UId": data_reply.get("id_str"),  # 被评论者ID
                "ScreenName": data_reply.get("name"),  # 被评论者昵称
                "ScreenAccount": data_reply.get("screen_name"),  # 被评论者账号
                "URL": 'https://twitter.com/' + data_reply.get("screen_name"),  # 被评论者主页URL

                "FollowerUId": comment.get('AuthorID'),  # 评论者id
                "FollowerScreenName": comment.get('AuthorNick'),  # 评论者昵称
                "FollowerAccount": comment.get('Author'),  # 评论者账号
                "FollowerURL": 'https://twitter.com/' + comment.get('Author'),  # 评论者主页URL

                "PublishTime": comment.get('PublishTime'),  # 评论发布时间
                "IsFriend": 0,  # 是否好友关系，0-是，1-否
                "Flag": 2,  # 数据来源  0：被关注；1：转发； 2：评论； 3：点赞；4：大爱；5：笑趴；6：哇；7：心碎；8：怒'
            }
            yield social_relation

    @staticmethod
    def make_social_relation_dict_bad(data_reply_to_users, comment):
        social_relation = {
            "wbParentId": comment.get("PNID"),  # 被评论帖子ID

            "UId": data_reply_to_users.get("user_id_choose"),
            "ScreenName": data_reply_to_users.get("user_nick_choose"),
            "ScreenAccount": data_reply_to_users.get("user_account_choose"),
            "URL": 'https://twitter.com/' + data_reply_to_users.get("account"),  # 被评论者主页URL

            "FollowerUId": comment.get('AuthorID'),  # 评论者id
            "FollowerScreenName": comment.get('AuthorNick'),  # 评论者昵称
            "FollowerAccount": comment.get('Author'),  # 评论者账号
            "FollowerURL": 'https://twitter.com/' + comment.get('Author'),  # 评论者主页URL

            "PublishTime": comment.get('PublishTime'),  # 评论发布时间
            "Flag": 2,  # 数据来源  0：被关注；1：转发； 2：评论； 3：点赞；4：大爱；5：笑趴；6：哇；7：心碎；8：怒'
        }
        return social_relation

    # 解析一级评论(因为帖子首页是Html,翻页是json，所以提取出来)
    def parse_comment(self, response):
        """解析一级评论(因为帖子首页是Html,翻页是json，所以提取出来)"""
        article_id = response.meta2.get("article_id")
        article_url = response.meta2.get("article_url")
        is_new = response.meta2.get("is_new")
        # 用于构造社交关系
        user_id_choose = response.meta2.get('user_id_choose')
        user_account_choose = response.meta2.get('user_account_choose')
        user_nick_choose = response.meta2.get('user_nick_choose')
        account = response.meta2.get('account')

        article_link = re.search('twitter.com(\S+)', article_url).group(1)
        # xpath_comments = "//li[contains(@class,'ThreadedConversation')][not(contains(@class,'-moreReplies'))]//div[contains(@class,'js-stream-tweet')]"
        # 1.当前页评论已显示二级评论(只取1级，后面取2级)； 2.当前页评论未显示二级评论
        xpath_comments = "//ol[@class='stream-items']/div[contains(@class,'ThreadedConversation-tweet')][1]/li[@data-item-type='tweet']//div[contains(@class,'js-stream-tweet')] | //ol[@class='stream-items']/li[@data-item-type='tweet']//div[contains(@class,'js-stream-tweet')]"
        comment_selectors = response.xpath(xpath_comments)
        for comment in comment_selectors:
            # 去除主页帖子内容不定时插在评论中间
            comment_link = comment.xpath("./@data-permalink-path").extract_first()
            if comment_link == article_link:
                continue
            # 是否终止采集评论
            is_end_comment = 0

            # 1.解析评论
            comment_id = comment.xpath("./@data-item-id").extract_first()  # 评论ID
            author = comment.xpath("./@data-screen-name").extract_first()  # 评论人账号
            author_nick = comment.xpath("./@data-name").extract_first()  # 评论人昵称
            author_id = comment.xpath("./@data-user-id").extract_first()  # 评论人ID
            # 评论内容(1.评论文本内容， 2.媒体内容， 3.外部内容(可以跳转到其他文章))
            xpath_content = ".//div[@class='js-tweet-text-container'] | .//div[@class='AdaptiveMediaOuterContainer'] | .//div[contains(@class,'u-block js-tweet-details-fixer')]"
            content = ''.join(comment.xpath(xpath_content).extract())
            # 评论内容语言代码
            content_language_code = response.xpath(".//p/@lang").extract_first()
            # 评论发布时间
            xpath_ptime = ".//div[@class='stream-item-header']//small[@class='time']//span[@data-time]/@data-time"
            ptime = comment.xpath(xpath_ptime).extract_first()
            if not ptime or not content:  # 没有内容和时间的评论丢弃
                continue
            p_time = int(ptime.strip())
            if self.increment_crawl and not is_new:
                current_time = datetime.datetime.now() - datetime.timedelta(hours=self.comment_time_interval)
                comment_current_time = int(current_time.timestamp())
                if p_time < comment_current_time:
                    is_end_comment = 1

            # 评论发布时间
            publish_time = str(datetime.datetime.fromtimestamp(p_time))
            # 评论回复数
            xpath_follow = ".//div[contains(@class,'stream-item-footer')]//div[contains(@class,'ProfileTweet-actionCountList')]//span[contains(@class,'ProfileTweet-action--reply')]/span[@class='ProfileTweet-actionCount']/@data-tweet-stat-count"
            follow = comment.xpath(xpath_follow).extract_first()
            follow_count = int(follow) if follow else 0
            # 评论点赞数
            xpath_agree = ".//div[contains(@class,'stream-item-footer')]//div[contains(@class,'ProfileTweet-actionCountList')]//span[contains(@class,'ProfileTweet-action--favorite')]/span[@class='ProfileTweet-actionCount']/@data-tweet-stat-count"
            agree = comment.xpath(xpath_agree).extract_first()
            agree_count = int(agree) if agree else 0

            # 被评论人等信息
            data_reply_to_users_json = comment.xpath("./@data-reply-to-users-json").extract_first()  # 评论人ID
            data_reply_to_users = json.loads(data_reply_to_users_json)
            if len(data_reply_to_users) > 1:
                data_reply_to_users_list = data_reply_to_users[1:]
            else:
                data_reply_to_users_list = data_reply_to_users

            comment_data = {
                # "news_url": article_url,
                "PNID": article_id,  # 帖子ID
                "ParentPCID": article_id,  # 父PCID/帖子ID
                "PCID": comment_id,  # 评论ID
                "Author": author,  # 评论人账号
                "AuthorNick": author_nick,  # 评论人昵称
                "AuthorID": author_id,  # 评论人ID
                "Homepage": 'https://twitter.com/' + author,  # 评论人主页链接
                "Content": content,  # 评论内容
                "PublishTime": publish_time,  # 评论发布时间
                "FollowCount": follow_count,  # 回复数
                "AgreeCount": agree_count,  # 点赞数
                "LanguageCode": content_language_code or 'en',  # 语言编码 response.meta2.get("LanguageCode")

                "Location": "",
                # 需要pop的参数：
                "data_reply_to_users": data_reply_to_users_list,  # 被评论人等信息
                "is_end_comment": is_end_comment,  # 终止评论翻页
                "NEW_RELATIONS": {
                    "user_id_choose": user_id_choose,
                    "account": account,
                    "user_account_choose": user_account_choose,
                    "user_nick_choose": user_nick_choose
                }
            }
            yield comment_data

            if is_end_comment:
                return []
            # 回复数大于0的，即存在二级评论
            if follow_count > 0:  # 解析二级评论
                comment_url = response.urljoin(comment_link)
                meta = copy.deepcopy(response.meta2)
                meta["commentator_account"] = author
                meta["comment_id"] = comment_id
                yield scrapy.Request(url=comment_url, callback=self.parse_secondary_comment_page, meta=meta)

    # 解析二级评论(因为二级评论首页是Html,翻页是json，所以提出来)
    def parse_secondary_comment(self, response):
        """解析二级评论(因为二级评论首页是Html,翻页是json，所以提出来)
        :param response:
        :return: yield 二级评论字典；二级评论块的最后一个还有显示更多回复(Url)
        """
        article_id = response.meta2.get("article_id")
        article_url = response.meta2.get("article_url")
        is_new = response.meta2.get("is_new")
        # 用于构造社交关系
        user_id_choose = response.meta2.get('user_id_choose')
        user_account_choose = response.meta2.get('user_account_choose')
        user_nick_choose = response.meta2.get('user_nick_choose')
        account = response.meta2.get('account')

        article_link = re.search('twitter.com(\S+)', article_url).group(1)
        # 解析规则相对于1级评论范围增大
        xpath_comments = "//ol[@class='stream-items']//li[@data-item-type='tweet']//div[contains(@class,'js-stream-tweet')]"
        comment_selectors = response.xpath(xpath_comments)
        for comment in comment_selectors:
            # 去除主页帖子内容不定时插在评论中间
            comment_link = comment.xpath("./@data-permalink-path").extract_first()
            if comment_link == article_link:
                continue
            # 是否终止采集二级评论
            is_end_comment = 0

            # 1.解析评论
            comment_id = comment.xpath("./@data-item-id").extract_first()  # 评论ID
            author = comment.xpath("./@data-screen-name").extract_first()  # 评论人账号
            author_nick = comment.xpath("./@data-name").extract_first()  # 评论人昵称
            author_id = comment.xpath("./@data-user-id").extract_first()  # 评论人ID

            # 评论内容(1.评论文本内容， 2.媒体内容， 3.外部内容(可以跳转到其他文章))
            xpath_content = ".//div[@class='js-tweet-text-container'] | .//div[@class='AdaptiveMediaOuterContainer'] | .//div[contains(@class,'u-block js-tweet-details-fixer')]"
            content = ''.join(comment.xpath(xpath_content).extract())
            # 评论内容语言代码
            content_language_code = response.xpath(".//p/@lang").extract_first()
            # 评论发布时间
            xpath_ptime = ".//div[@class='stream-item-header']//small[@class='time']//span[@data-time]/@data-time"
            ptime = comment.xpath(xpath_ptime).extract_first()
            if not ptime or not content:  # 没有内容和时间的评论丢弃
                continue
            p_time = int(ptime.strip())
            # 这里判定一次时间(距当前时间超过1小时的评论丢弃)
            if self.increment_crawl and not is_new:
                current_time = datetime.datetime.now() - datetime.timedelta(hours=self.comment_time_interval)
                comment_current_time = int(current_time.timestamp())
                if p_time < comment_current_time:
                    is_end_comment = 1

            publish_time = str(datetime.datetime.fromtimestamp(p_time))
            # 评论回复数
            xpath_follow = ".//div[contains(@class,'stream-item-footer')]//div[contains(@class,'ProfileTweet-actionCountList')]//span[contains(@class,'ProfileTweet-action--reply')]/span[@class='ProfileTweet-actionCount']/@data-tweet-stat-count"
            follow = comment.xpath(xpath_follow).extract_first()
            follow_count = int(follow) if follow else 0
            # 评论点赞数
            xpath_agree = ".//div[contains(@class,'stream-item-footer')]//div[contains(@class,'ProfileTweet-actionCountList')]//span[contains(@class,'ProfileTweet-action--favorite')]/span[@class='ProfileTweet-actionCount']/@data-tweet-stat-count"
            agree = comment.xpath(xpath_agree).extract_first()
            agree_count = int(agree) if agree else 0

            # 被评论人等信息
            data_reply_to_users_json = comment.xpath("./@data-reply-to-users-json").extract_first()  # 评论人ID
            data_reply_to_users = json.loads(data_reply_to_users_json)

            if len(data_reply_to_users) > 1:
                data_reply_to_users_list = data_reply_to_users[1:]
            else:
                data_reply_to_users_list = data_reply_to_users

            comment_data = {
                # "news_url": article_url,
                "PNID": article_id,  # 帖子ID
                "ParentPCID": article_id,  # 父PCID/帖子ID
                "PCID": comment_id,  # 评论ID
                "Author": author,  # 评论人账号
                "AuthorNick": author_nick,  # 评论人昵称
                "AuthorID": author_id,  # 评论人ID
                "Homepage": 'https://twitter.com/' + author,  # 评论人主页链接
                "Content": content,  # 评论内容
                "PublishTime": publish_time,  # 评论发布时间
                "FollowCount": follow_count,  # 回复数
                "AgreeCount": agree_count,  # 点赞数
                "LanguageCode": content_language_code or 'en',  # 语言编码 response.meta2.get("LanguageCode")

                "Location": "",
                # 需要pop的参数：
                "data_reply_to_users": data_reply_to_users_list,  # 被评论人等信息
                "is_end_comment": is_end_comment,
                "NEW_RELATIONS": {
                    "user_id_choose": user_id_choose,
                    "account": account,
                    "user_account_choose": user_account_choose,
                    "user_nick_choose": user_nick_choose
                }
            }
            yield comment_data

            if is_end_comment:
                return []
            # 另外？条回复
            xpath_show_more_replies = "./ancestor::ol[@class='stream-items']//li[@class='ThreadedConversation-moreReplies'][@data-element-context='show_more_button']/@data-expansion-url"
            show_more_replies = comment.xpath(xpath_show_more_replies).extract_first()
            if show_more_replies:
                show_more_replies_url = 'https://twitter.com' + show_more_replies
                meta = copy.deepcopy(response.meta2)
                yield scrapy.Request(url=show_more_replies_url, callback=self.parse_show_more_more_replies, meta=meta)

    def requests_of_get(self, url):
        for i in range(self.max_failure_time):
            try:
                res = requests.get(url=url, headers=self.common_headers)
                return res
            except Exception as e:
                print(f'第{i}次下载page of html or json失败，Url:{url}，失败原因{e}')
        else:
            return

    @staticmethod
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
