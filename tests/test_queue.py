from . import *


class TestQueueBasics(TestCase):

    def setUp(self):
        self.redis = Redis()
        self.queue = Queue(self.redis, name='queue_basics')
        
        existing = self.redis.keys('queue_basics:*')
        if existing:
            self.redis.delete(keys)

    def test_basic_submit(self):

        jid = self.queue.submit({})
        self.assertEqual(jid, 'queue_basics:task:1')

        self.assertEqual(self.redis.hget(jid, 'status'), 'pending')
        self.assertEqual(self.redis.hget(jid, 'priority'), '1000')



