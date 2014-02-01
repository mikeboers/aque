from . import *


class TestWorkerBasics(TestCase):

    def setUp(self):

        self.redis = Redis()
        self.queue = Queue(self.redis, name='worker_basics')
        
        existing = self.redis.keys('worker_basics:*')
        if existing:
            self.redis.delete(*existing)

    def test_open_tasks(self):

        b = Task()
        c = Task()
        a = Task(children=[b, c])

        self.queue.submit(a)

        worker = Worker(self.queue)
        self.assertEqual([b, c], list(worker.iter_open_tasks()))

        self.redis.hset(b.id, 'status', 'success')
        self.assertEqual([c], list(worker.iter_open_tasks()))

        self.redis.hset(c.id, 'status', 'success')
        self.assertEqual([a], list(worker.iter_open_tasks()))






