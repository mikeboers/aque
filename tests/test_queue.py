from . import *


class TestQueueBasics(TestCase):

    def setUp(self):
        self.name = self.__class__.__name__
        self.queue = Queue(name=self.name)
        self.broker = self.queue.broker
        self.redis = self.broker._redis
        
        existing = self.redis.keys(self.name + ':*')
        if existing:
            self.redis.delete(*existing)

    def test_basic_submit(self):

        jid = self.queue.submit({})
        self.assertEqual(jid, 'TestQueueBasics:task:1')

        self.assertEqual(self.broker.get(jid, 'status'), 'pending')
        self.assertEqual(self.broker.get(jid, 'priority'), 1000)

    def test_manual_child_submit(self):

        c = Task()
        self.queue.submit(c)

        p = Task(children=[c])
        self.queue.submit(p)

        self.assertEqual(c.id, 'TestQueueBasics:task:1')
        self.assertEqual(p.id, 'TestQueueBasics:task:2')

    def test_auto_child_submit(self):

        c = Task()
        p = Task(children=[c])

        self.queue.submit(p)

        self.assertEqual(p.id, 'TestQueueBasics:task:1')
        self.assertEqual(c.id, 'TestQueueBasics:task:2')






