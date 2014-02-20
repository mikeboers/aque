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

        f = self.queue.submit_ex(**a)

        def open_names():
            open_tasks = list(worker.iter_open_tasks())
            open_names = [t['name'] for tid, t in open_tasks]
            return open_names

        worker = Worker(self.broker)
        self.assertEqual(open_names(), ['b', 'c'])

        self.broker.mark_as_complete(f.children[0].id, 'result')
        self.assertEqual(open_names(), ['c'])

        self.broker.mark_as_complete(f.children[1].id, 'result')
        pprint(self.broker._tasks)

        self.assertEqual(open_names(), ['a'])






