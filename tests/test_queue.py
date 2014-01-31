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

        self.assertEqual(self.redis.hget(jid, 'status'), 'pending')
        self.assertEqual(self.redis.hget(jid, 'priority'), '1000')

    def test_manual_child_submit(self):

        c = Task()
        self.queue.submit(c)

        p = Task(children=[c])
        self.queue.submit(p)

        self.assertEqual(c.id, 'queue_basics:task:1')
        self.assertEqual(p.id, 'queue_basics:task:2')

    def test_auto_child_submit(self):

        c = Task()
        p = Task(children=[c])

        self.queue.submit(p)

        self.assertEqual(p.id, 'queue_basics:task:1')
        self.assertEqual(c.id, 'queue_basics:task:2')






