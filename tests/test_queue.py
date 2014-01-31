from . import *


class TestQueueBasics(TestCase):

    def setUp(self):
        self.redis = Redis()
        self.queue = Queue(self.redis, name='queue_basics')
        
        existing = self.redis.keys('queue_basics:*')
        if existing:
            self.redis.delete(*existing)

    def test_basic_submit(self):

        jid = self.queue.submit({})
        self.assertEqual(jid, 'queue_basics:task:1')

        self.assertEqual(self.redis.hget(jid + ':dynamic', 'status'), 'pending')
        self.assertEqual(self.redis.hget(jid + ':static', 'priority'), '1000')



