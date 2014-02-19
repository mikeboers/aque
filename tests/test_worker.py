from . import *


class TestWorkerBasics(TestCase):

    def setUp(self):
        self.name = self.__class__.__name__
        self.broker = LocalBroker()
        self.queue = Queue(broker=self.broker)

    def test_open_tasks(self):

        b = {'name': 'b'}
        c = {'name': 'c'}
        a = {'name': 'a', 'children': [b, c]}

        self.queue.submit_ex(task=a)

        worker = Worker(self.broker)
        open_tasks = list(worker.iter_open_tasks())
        open_names = [t['name'] for t in open_tasks]
        self.assertEqual(open_names, ['b', 'c'])

        self.broker.mark_as_complete(b.id, 'status', 'success')
        self.assertEqual([c], list(worker.iter_open_tasks()))

        self.redis.hset(c.id, 'status', 'success')
        self.assertEqual([a], list(worker.iter_open_tasks()))






