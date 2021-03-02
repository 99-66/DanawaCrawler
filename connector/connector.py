import random

from redis import Redis
from pymongo import MongoClient

import config


class RedisConnector:
    connectionString = config.REDIS

    def __init__(self, encoding='utf-8'):
        self.client = self._default()
        self.encoding = encoding

    @classmethod
    def _default(cls):
        return Redis(host=cls.connectionString['HOST'],
                     port=cls.connectionString['PORT'],
                     password=cls.connectionString['PASSWORD'],
                     db=cls.connectionString['DB'],
                     decode_responses=True)

    def conn(self):
        return self.client


class MongoDBConnector:
    mongodb = config.MONGODB

    def __init__(self, client=None):
        if client:
            self.client = client
        else:
            self.client = MongoClient(self._default())

        self.collection = self._collection()

    @classmethod
    def _default(cls):
        conn = f'mongodb://{cls.mongodb["USER"]}:{cls.mongodb["PASSWORD"]}@{cls.mongodb["HOST"]}:{cls.mongodb["PORT"]}/'
        if cls.mongodb['SSL'] and cls.mongodb['SSL'] is True:
            conn = f'{conn}?ssl=true'
            if cls.mongodb['SSL_CA_CERTS']:
                conn = f'{conn}&ssl_ca_certs={cls.mongodb["SSL_CA_CERTS"]}'
        else:
            conn = f'{conn}?ssl=false'

        if cls.mongodb['REPLICA_SET']:
            conn = f'{conn}&replicaSet={cls.mongodb["REPLICA_SET"]}'

        return conn

    @classmethod
    def _collection(cls):
        return cls.mongodb['COLLECTION']

    def conn(self):
        return self.client

    def random_keyword(self):
        ret = []
        collection = self.client[self.collection]['keywords']
        cursor = collection.find({})

        for keyword in cursor:
            if 'type' in keyword:
                if keyword['type'] == 'product':
                    ret.append(keyword['_id'])

        random.shuffle(ret)

        return ret
