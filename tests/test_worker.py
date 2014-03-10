from aque.brokers.memory import MemoryBroker
from . import *


class TestWorkerBasics(BrokerTestCase):

    def test_open_tasks(self):

        b = {'name': 'b', 'func': tuple}
        c = {'name': 'c', 'func': tuple}
        a = {'name': 'a', 'func': tuple, 'dependencies': [b, c]}

        f = self.queue.submit_ex(**a)

        def open_names():
            open_tasks = list(self.worker.iter_open_tasks())
            open_names = set(t['name'] for t in open_tasks)
            return open_names

        self.assertEqual(open_names(), set(['b', 'c']))

        self.worker.run_one()
        self.assertEqual(open_names(), set(['c']))

        self.worker.run_one()
        self.assertEqual(open_names(), set(['a']))

