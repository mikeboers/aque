from . import *


class TestWorkerBasics(TestCase):

    def setUp(self):

        self.name = self.__class__.__name__
        self.broker = Broker(name=self.name)
        self.redis = self.broker._redis
        self.queue = Queue(broker=self.broker)
        
        existing = self.redis.keys(self.name + ':*')
        if existing:
            self.redis.delete(*existing)

    def test_open_tasks(self):

        b = Task()
        c = Task()
        a = Task(children=[b, c])

        self.queue.submit(a)

        worker = Worker(self.broker)
        self.assertEqual([b, c], list(worker.iter_open_tasks()))

        self.redis.hset(b.id, 'status', 'success')
        self.assertEqual([c], list(worker.iter_open_tasks()))

        self.redis.hset(c.id, 'status', 'success')
        self.assertEqual([a], list(worker.iter_open_tasks()))






