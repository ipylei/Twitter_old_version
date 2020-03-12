# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class Xj13TwitterItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class ArticleItem(scrapy.Item):
    PNID = scrapy.Field()
    Url = scrapy.Field()
    Author = scrapy.Field()
    AuthorNick = scrapy.Field()
    PublishTime = scrapy.Field()
    Content = scrapy.Field()
    Content_search = scrapy.Field()
    CommentCount = scrapy.Field()
    ForwardNum = scrapy.Field()
    LanguageCode = scrapy.Field()
    ClickCount = scrapy.Field()

    Title = scrapy.Field()
    Abstract = scrapy.Field()
    Keywords = scrapy.Field()

    VideoUrl = scrapy.Field()
    MediaSourceUrl = scrapy.Field()
    # 真
    origin_author_id = scrapy.Field()
    origin_author = scrapy.Field()
    origin_author_nickname = scrapy.Field()
    # 伪
    user_id = scrapy.Field()
    user_account = scrapy.Field()
    user_nick = scrapy.Field()

    is_new = scrapy.Field()
    account = scrapy.Field()


class CommentItem(scrapy.Item):
    news_url = scrapy.Field()

    PNID = scrapy.Field()
    ParentPCID = scrapy.Field()
    PCID = scrapy.Field()
    Author = scrapy.Field()
    AuthorNick = scrapy.Field()
    AuthorID = scrapy.Field()
    Homepage = scrapy.Field()
    Content = scrapy.Field()
    PublishTime = scrapy.Field()
    AgreeCount = scrapy.Field()
    FollowCount = scrapy.Field()
    LanguageCode = scrapy.Field()

    Location = scrapy.Field()


class SocialRelationItem(scrapy.Item):
    # ListSocialRelation = scrapy.Field()
    wbParentId = scrapy.Field()  # 被评论帖子ID

    UId = scrapy.Field()  # 被评论者ID
    ScreenName = scrapy.Field()  # 被评论者昵称
    ScreenAccount = scrapy.Field()  # 被评论者账号
    URL = scrapy.Field()  # 被评论者主页URL

    FollowerUId = scrapy.Field()  # 评论者id
    FollowerScreenName = scrapy.Field()  # 评论者昵称
    FollowerAccount = scrapy.Field()  # 评论者账号
    FollowerURL = scrapy.Field()  # 评论者主页URL

    PublishTime = scrapy.Field()  # 评论发布时间
    IsFriend = scrapy.Field()  # 是否好友关系，0-是，1-否
    Flag = scrapy.Field()  # 数据来源  0：被关注；1：转发； 2：评论； 3：点赞；4：大爱；5：笑趴；6：哇；7：心碎；8：怒'
    Platform = scrapy.Field()  # 平台


class ArticleCount(scrapy.Item):
    Url = scrapy.Field()
    count = scrapy.Field()
    end_collect = scrapy.Field()

