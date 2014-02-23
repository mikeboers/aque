from pprint import pprint
from unittest import TestCase

from redis import Redis

from aque import Queue, Future, execute
from aque.brokers.local import LocalBroker
from aque.brokers.redis import RedisBroker
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

