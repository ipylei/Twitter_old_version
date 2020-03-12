# -*- coding: utf-8 -*-
from twitter.twitter_relations import Twitter


def start_consume_relations():
    count = 0
    while True:  # 社交关系通道是否存活(不存在就消费直到遇见pipelines中推过来的is_end, 存活就不再消费)
        try:
            twitter_obj = Twitter()
            twitter_obj.login()
            is_end = twitter_obj.start_crawl()
            if is_end:  # 不再从redis中获取社交关系
                print('本轮社交关系获取完毕...')
                break
        except Exception as e:
            print(e)
            count += 1
            pass
        print('即将重新登录...')
    print('今天已采集完社交关系...')


if __name__ == '__main__':
    start_consume_relations()
