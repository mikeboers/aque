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

        f = self.queue.submit_ex(task={})
        self.assertEqual(f.id, 'TestQueueBasics:task:1')

        self.assertEqual(self.broker.get(f.id, 'status'), 'pending')
        self.assertEqual(self.broker.get(f.id, 'priority'), 1000)

    def test_manual_child_submit(self):
        return
        cf = self.queue.submit_ex(task={})
        pf = self.queue.submit_ex(task={'children': [cf.id]})

        self.assertEqual(c.id, 'TestQueueBasics:task:1')
        self.assertEqual(p.id, 'TestQueueBasics:task:2')

    def test_auto_child_submit(self):

        f = self.queue.submit_ex(task={'children': [{}]})

        self.assertEqual(f.id, 'TestQueueBasics:task:1')
        self.assertEqual(f.children[0].id, 'TestQueueBasics:task:2')






