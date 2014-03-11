from . import *


class TestDefaults(BrokerTestCase):

    def test_basic_submit(self):
        f = self.queue.submit_ex()
        self.assertTrue(f.id)
        self.assertEqual(self.broker.fetch(f.id)['status'], 'pending')
        self.assertEqual(self.broker.fetch(f.id)['priority'], 1000)

    def test_manual_child_submit_by_id(self):
        cf = self.queue.submit_ex()
        pf = self.queue.submit_ex(dependencies=[cf.id])
        self.assertTrue(pf.id > cf.id)

    def test_manual_child_submit_by_future(self):
        cf = self.queue.submit_ex()
        pf = self.queue.submit_ex(dependencies=[cf])
        self.assertIs(cf, list(pf.iter())[1])
        self.assertTrue(pf.id > cf.id)

    def test_auto_child_submit(self):
        f = self.queue.submit_ex(dependencies=[{}])
        self.assertTrue(f.id > list(f.iter())[1].id, 1)
