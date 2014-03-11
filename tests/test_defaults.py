from . import *


class TestDefaults(BrokerTestCase):

    def test_basic_submit(self):
        f = self.queue.submit_ex(name=self.id() + '.0', pattern=None)
        self.assertTrue(f.id)
        self.assertEqual(self.broker.fetch(f.id)['status'], 'pending')
        self.assertEqual(self.broker.fetch(f.id)['priority'], 1000)

    def test_manual_child_submit_by_id(self):
        cf = self.queue.submit_ex(name=self.id() + '.0', pattern=None)
        pf = self.queue.submit_ex(name=self.id() + '.1', pattern=None, dependencies=[cf.id])
        self.assertTrue(pf.id > cf.id)

    def test_manual_child_submit_by_future(self):
        cf = self.queue.submit_ex(name=self.id() + '.0', pattern=None)
        pf = self.queue.submit_ex(name=self.id() + '.1', pattern=None, dependencies=[cf])
        self.assertIs(cf, list(pf.iter())[1])
        self.assertTrue(pf.id > cf.id)

    def test_auto_child_submit(self):
        f = self.queue.submit_ex(name=self.id() + '.0', pattern=None, dependencies=[{
            'name': self.id() + '.1',
            'pattern': None,
        }])
        self.assertTrue(f.id > list(f.iter())[1].id, 1)
