from . import *


class TestQueueBasics(BrokerTestCase):

    def test_basic_submit(self):
        f = self.queue.submit_ex()
        self.assertEqual(f.id, 1)
        self.assertEqual(self.broker.fetch(f.id)['status'], 'pending')
        self.assertEqual(self.broker.fetch(f.id)['priority'], 1000)


    def test_manual_child_submit_by_id(self):

        cf = self.queue.submit_ex()
        pf = self.queue.submit_ex(dependencies=[cf.id])

        self.assertEqual(cf.id, 1)
        self.assertEqual(pf.id, 2)

    def test_manual_child_submit_by_future(self):

        cf = self.queue.submit_ex()
        pf = self.queue.submit_ex(dependencies=[cf])

        self.assertIs(cf, list(pf.iter())[1])
        self.assertEqual(cf.id, 1)
        self.assertEqual(pf.id, 2)


    def test_auto_child_submit(self):

        f = self.queue.submit_ex(dependencies=[{}])

        self.assertEqual(list(f.iter())[1].id, 1)
        self.assertEqual(f.id, 2)






