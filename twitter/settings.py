# -*- coding: utf-8 -*-

# Scrapy settings for twitter project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'twitter'

SPIDER_MODULES = ['twitter.spiders']
NEWSPIDER_MODULE = 'twitter.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'twitter (+http://www.yourdomain.com)'
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.75 Safari/537.36'

# Obey robots.txt rules
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
# CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
# CONCURRENT_REQUESTS_PER_DOMAIN = 16
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
    # 'Host': 'twitter.com',
    # 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.75 Safari/537.36',
    # 'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
    # 'Accept-Language': 'zh-CN,zh;q=0.9'
# }

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    'twitter.middlewares.Xj13TwitterSpiderMiddleware': 543,
# }

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
   'twitter.middlewares.Xj13TwitterDownloaderMiddleware': 543,
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
# }

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
ITEM_PIPELINES = {
    # 'twitter.pipelines.Xj13TwitterPipeline': 300,
    # 'twitter.pipelines.MySQLPipeline': 301,
}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# The initial download delay
# AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
# LOG_FILE = 'log.txt'
# LOG_LEVEL = 'WARNING'

URLLENGTH_LIMIT = 10000
# 本机MQ配置
# MQ_HOST = '127.0.0.1'
# MQ_PORT = 5672
# MQ_USERNAME = 'guest'
# MQ_PASSWORD = 'guest'
# MQ_QUEUE_ARTICLES = 'SX_Temp_articles'  # 帖子队列
# MQ_QUEUE_COMMENTS = 'SX_Temp_comments'  # 评论队列
# MQ_QUEUE_RELATIONS = 'IDC_YQ_SocialRelations'  # 社交关系队列

# MQ配置
# MQ_HOST = '127.0.0.1'
MQ_HOST = '159.138.29.14'
MQ_PORT = 5672
MQ_USERNAME = 'admin'
MQ_PASSWORD = '123456'
MQ_QUEUE_ARTICLES = 'BD_YQ_Twitter'  # 帖子队列
MQ_QUEUE_COMMENTS = 'SX_Temp'  # 评论队列
MQ_QUEUE_RELATIONS = 'IDC_YQ_SocialRelations'  # 社交关系队列


DB_KEY = 'BD_YQ_Twitter'  # 帖子/评论DB_KEY
DB_KEY_RELATIONS = 'BD_YQ_SocialRelations'  # 社交关系DB_KEY

USER_ACCOUNTS_URL = 'http://api.osint.cdrwsoft.com/monitor-service/api/GetAccounts/GetAccountsForCrawler'
PlatformName = 'twitter'
ProjectName = 'WX'

# Redis数据库配置
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_KEY = MQ_QUEUE_ARTICLES + '_Bloom'
REDIS_KEY_URLS = 'Urls_' + MQ_QUEUE_ARTICLES  # 社交关系REDIS通道
REDIS_KEY_URLS_STATUS = REDIS_KEY_URLS + '_Status'  # 社交关系通道状态(是否存活)
REDIS_KEY_URLS_BLOOM = MQ_QUEUE_ARTICLES + '_SocialRelations_Bloom'  # 社交关系去重

# 是否下载图片/视频
IMAGE_COLLECT_ENABLE = True
VIDEO_COLLECT_ENABLE = True

# 用户信息选择  1、传过来的账号、昵称；2、采集目标的真实账号、昵称；3、其他、帖子原作者的真实账号、昵称
USER_MSG_CHOOSE = 1

# Twitter用户
USERNAME = 'xStone15'
PASSWORD = 'Aaa123!!'
TELEPHONE = '17760519743'
