from . import *


class TestQueueBasics(RedisTestCase):

    def test_basic_submit(self):

        f = self.queue.submit_ex()
        self.assertEqual(f.id, 'TestQueueBasics:task:1')

        self.assertEqual(self.broker.get(f.id, 'status'), 'pending')
        self.assertEqual(self.broker.get(f.id, 'priority'), 1000)

    def test_manual_child_submit_by_id(self):

        cf = self.queue.submit_ex()
        pf = self.queue.submit_ex(children=[cf.id])

        self.assertEqual(cf.id, 'TestQueueBasics:task:1')
        self.assertEqual(pf.id, 'TestQueueBasics:task:2')

    def test_manual_child_submit_by_future(self):

        cf = self.queue.submit_ex()
        pf = self.queue.submit_ex(children=[cf])

        self.assertIs(cf, pf.children[0])
        self.assertEqual(cf.id, 'TestQueueBasics:task:1')
        self.assertEqual(pf.id, 'TestQueueBasics:task:2')


    def test_auto_child_submit(self):

        f = self.queue.submit_ex(children=[{}])

        self.assertEqual(f.id, 'TestQueueBasics:task:1')
        self.assertEqual(f.children[0].id, 'TestQueueBasics:task:2')






