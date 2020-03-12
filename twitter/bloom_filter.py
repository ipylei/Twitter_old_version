# -*- coding: utf-8 -*-
import redis

REDIS_HOST = '127.0.0.1'
# REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
REDIS_KEY = 'twitter'

BLOOM_FILTER_HASH_NUMBER = 6  # 指定哈希函数个数
BLOOM_FILTER_BIT = 30  # 指定数组长度


class RedisClient(object):
    """Redis连接"""

    def __init__(self, host=REDIS_HOST, port=REDIS_PORT, redis_db=REDIS_DB):
        self.db = redis.StrictRedis(host=host, port=port, db=redis_db)

    def close(self):
        self.db.close()

    def push(self, key, value):
        self.db.lpush(key, value)

    def pop(self, key):
        return self.db.rpop(key).decode()

    def empty(self, key):
        return self.db.llen(key) == 0


class HashMap(object):
    def __init__(self, m, seed):
        """
        :param m: m位数组的位数
        :param seed: 种子值seed
        """
        self.m = m
        self.seed = seed

    def hash(self, value):
        ret = 0
        for i in range(len(value)):
            ret = self.seed * ret + ord(value[i])  # 对字符串的每一位进行映射，整合到一个值上
        # 数据库是从下标0开始的，所以要减1
        return (self.m - 1) & ret  # 将这个数值和m进行按位与运算，即可获取到m位数组的映射结果(0或者其他值)


class BloomFilter(object):
    """布隆过滤器"""

    def __init__(self, bit=BLOOM_FILTER_BIT, hash_number=BLOOM_FILTER_HASH_NUMBER,
                 server=RedisClient(), bloom_key=REDIS_KEY):
        self.m = 1 << bit  # m位数组
        self.seeds = range(hash_number)  # 种子个数
        self.maps = [HashMap(self.m, seed) for seed in self.seeds]
        self.server = server
        self.bloom_key = bloom_key

    def exists(self, value):
        """判断字符串是否存在"""
        if not value:
            return False
        exist = 1
        for hash_map in self.maps:
            offset = hash_map.hash(value)  # 一个数值(按位与运算的结果),下面作为偏移量
            exist = exist & self.server.db.getbit(self.bloom_key, offset)  # 每个偏移都与数据库种对应的位置上的值(1 or 0)进行对比
        return exist

    def insert(self, value):
        """插入一个字符串"""
        for f in self.maps:
            offset = f.hash(value)
            self.server.db.setbit(self.bloom_key, offset, 1)


if __name__ == '__main__':
    bloom = BloomFilter()
    bloom.insert('Hello')
    bloom.insert('Hello')
    bloom.insert('Hello2')
    bloom.insert('World')
    bloom.insert('Python')
    result = bloom.exists('Hello')
    print(bool(result))
    result = bloom.exists('Python')
    print(bool(result))
    result = bloom.exists('Hello2')
    print(bool(result))
    result = bloom.exists('')
    print(bool(result))
