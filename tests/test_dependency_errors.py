from . import *


class TestDependencyErrors(BrokerTestCase):

    def test_missing_dependency(self):

        f = self.queue.submit_ex(tuple, dependencies=[123456])
        self.worker.run_to_end()
        
        self.assertRaises(DependencyError, f.result)

    def test_failed_dependency(self):

        a = self.queue.submit(int, 'not an int')
        b = self.queue.submit_ex(tuple, dependencies=[a])

        self.worker.run_to_end()

        self.assertRaises(ValueError, a.result)
        self.assertRaises(PreconditionFailed, b.result)
        
