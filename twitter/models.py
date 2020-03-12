# -*- coding: utf-8 -*-


import time

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

MYSQL_HOST = '127.0.0.1'
MYSQL_DATABASE = 'twitter'
MYSQL_PORT = 3306
MYSQL_USERNAME = 'root'
MYSQL_PASSWORD = '123456'

# 创建连接
# connect_url = "mysql+pymysql://root:123456@localhost:3306/zhiku"
connection_url = "mysql+pymysql://{}:{}@{}:{}/{}".format(MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_HOST,
                                                         MYSQL_PORT, MYSQL_DATABASE)
engine = create_engine(connection_url, encoding='utf-8', echo=True)

# 生成orm基类
Base = declarative_base()
Session_class = sessionmaker(bind=engine)  # 创建与数据库的会话，class,不是实例
Session = Session_class()  # 生成session实例


class ArticleSeed(Base):
    """文章表"""
    __tablename__ = 'twitter_article'  # 表名

    id = Column(Integer, primary_key=True)
    PNID = Column(String(500), nullable=False, comment='新闻ID')
    Url = Column(String(500), unique=True, nullable=False, comment='url')
    Author = Column(String(500), nullable=False, comment='作者')
    AuthorNick = Column(String(500), nullable=False, comment='昵称')
    PublishTime = Column(DateTime, comment='发布时间')
    Content = Column(Text, nullable=False, comment='内容')
    CommentCount = Column(Integer, comment='评论数')
    ForwardNum = Column(Integer, comment='转推数')
    ClickCount = Column(Integer, comment='点击数')
    LanguageCode = Column(String(500), nullable=False, comment='语言编码')
    Title = Column(String(500), nullable=False, comment='标题')
    Abstract = Column(Text, default='', comment='摘要')
    Keywords = Column(String(500), default='', comment='关键字')

    def save(self):
        Session.add(self)
        Session.commit()

    @staticmethod
    def update():
        try:
            Session.commit()
        except:
            Session.rollback()


class CommentSeed(Base):
    """评论表"""
    __tablename__ = 'twitter_comment'  # 表名

    id = Column(Integer, primary_key=True)
    PNID = Column(String(500), nullable=False, comment='帖子ID')
    ParentPCID = Column(String(500), nullable=False, comment='父PCID/帖子ID')
    PCID = Column(String(500), nullable=False, comment='评论的ID')
    Author = Column(String(500), nullable=False, comment='作者')
    AuthorNick = Column(String(500), nullable=False, comment='作者昵称')
    AuthorID = Column(String(500), nullable=False, comment='作者ID')
    Homepage = Column(String(500), nullable=False, comment='url')
    PublishTime = Column(DateTime, comment='发布时间')
    Content = Column(Text, nullable=False, comment='内容')
    FollowCount = Column(Integer, comment='回复数')
    AgreeCount = Column(Integer, comment='点赞数')
    LanguageCode = Column(String(500), nullable=False, comment='语言编码')
    Location = Column(String(500), nullable=False, comment='url')

    def save(self):
        Session.add(self)
        Session.commit()


class SocialRelationSeed(Base):
    """社交关系表"""
    __tablename__ = 'twitter_social_relation'  # 表名
    id = Column(Integer, primary_key=True)
    ListSocialRelation = Column(LONGTEXT, nullable=False, comment='社交关系')

    def save(self):
        Session.add(self)
        Session.commit()


if __name__ == '__main__':
    Base.metadata.create_all(engine)  # 创建表结构
    ulist = [
        'https://twitter.com/zh_bitterwinter/status/1179370762194558980',
        'https://twitter.com/RFAChinese/status/1157883977740406784',
        'https://twitter.com/luxunbot25/status/1182307070579744768',
        'https://twitter.com/DanHongTang/status/1184075740519813121',
        'https://twitter.com/luxunbot25/status/1182257094482903040',
        'https://twitter.com/zh_bitterwinter/status/1179276642805522434',
        'https://twitter.com/luxunbot25/status/1179320955744788481',
        'https://twitter.com/There4I/status/1163302602190262273',
        'https://twitter.com/ChineseWSJ/status/1180769919190945793',
        'https://twitter.com/fangshimin/status/1183571066511220742',
        'https://twitter.com/Tonyworld15/status/1163130197547208704',
        'https://twitter.com/nytchinese/status/1184091555658903552',
        'https://twitter.com/dw_chinese/status/1180056110306402304',
        'https://twitter.com/NewCenturyBaopu/status/1163276644343140352',
        'https://twitter.com/fangshimin/status/1181110032974241793',
        'https://twitter.com/dw_chinese/status/1163396662678740996',
        'https://twitter.com/nytchinese/status/1184077397525590016']
    objs = Session.query(ArticleSeed).all()
    for obj in objs:
        if obj.Url not in ulist:
            print(obj.Url)
