from . import *

class TestBrokerBasics(TestCase):

    def setUp(self):

        self.redis = Redis()
        self.name = self.__class__.__name__
        self.broker = Broker(redis=self.redis, name=self.name)
        
        existing = self.redis.keys(self.name + ':*')
        if existing:
            self.redis.delete(*existing)

    def test_new_task_ids(self):
        self.assertEqual(self.broker.new_task_id(), 'TestBrokerBasics:task:1')
        self.assertEqual(self.broker.new_task_id(), 'TestBrokerBasics:task:2')

    def test_basic_get_set(self):

        tid = self.broker.new_task_id()
        self.broker.set(tid, 'key', 'value')
        self.assertEqual(self.broker.get(tid, 'key'), 'value')

        self.broker.setmany(tid, {'bool': True, 'list': [1, 2, 3]})
        self.assertEqual(self.broker.getall(tid), {
            'key': 'value',
            'bool': True,
            'list': [1, 2, 3],
        })
