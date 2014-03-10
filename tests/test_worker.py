from aque.brokers.memory import MemoryBroker
from . import *


class TestWorkerBasics(TestCase):

    def setUp(self):
        self.name = self.__class__.__name__
        self.broker = MemoryBroker()
        self.queue = Queue(broker=self.broker)

    def test_open_tasks(self):

        b = {'name': 'b'}
        c = {'name': 'c'}
        a = {'name': 'a', 'dependencies': [b, c]}

        f = self.queue.submit_ex(**a)

        def open_names():
            open_tasks = list(worker.iter_open_tasks())
            open_names = set(t['name'] for t in open_tasks)
            return open_names

        worker = Worker(self.broker)
        self.assertEqual(open_names(), set(['b', 'c']))

        self.broker.mark_as_complete(list(f.iter())[1].id, 'result')
        self.assertEqual(open_names(), set(['c']))

        self.broker.mark_as_complete(list(f.iter())[2].id, 'result')
        self.assertEqual(open_names(), set(['a']))

