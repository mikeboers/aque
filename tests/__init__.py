from pprint import pprint
from unittest import TestCase

import sqlalchemy as sa
import psycopg2 as pg2
from redis import Redis

from aque import Queue, Future, execute
from aque.brokers.local import LocalBroker
from aque.brokers.redis import RedisBroker
from aque.brokers.postgres import PGBroker, literal
from aque.exceptions import DependencyError, TaskIncomplete, TaskError
from aque.worker import Worker


class LocalTestCase(TestCase):

    def setUp(self):
        self.name = self.__class__.__name__
        self.broker = self.get_broker()
        self.queue = Queue(name=self.name, broker=self.broker)

    def get_broker(self):
        return LocalBroker()


class RedisTestCase(LocalTestCase):

    def setUp(self):
        self.redis = Redis()
        super(RedisTestCase, self).setUp()
        existing = self.redis.keys(self.name + ':*')
        if existing:
            self.redis.delete(*existing)

    def get_broker(self):
        return RedisBroker(self.name, self.redis)


class PGTestCase(LocalTestCase):

    def setUp(self):

        self.db_name = 'aque_' + self.__class__.__name__.lower()

        with pg2.connect(database='postgres') as conn:
            conn.set_isolation_level(0)
            with conn.cursor() as cur:
                cur.execute('DROP DATABASE IF EXISTS %s' % self.db_name)
                cur.execute('CREATE DATABASE %s' % self.db_name)

        super(PGTestCase, self).setUp()


    def get_broker(self):
        return PGBroker(database=self.db_name)


