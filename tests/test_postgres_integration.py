from . import *

import threading


class TestPGIntegration(PGTestCase):

    def setUp(self):
        super(TestPGIntegration, self).setUp()
        self.thread = threading.Thread(target=self.worker_target)
        self.thread.start()

    def worker_target(self):
        self.worker = Worker(self.broker)
        self.worker.run_forever()

    def tearDown(self):
        self.worker.stop()
        self.thread.join()

    def test_single_generic_task(self):
        f = self.queue.submit(str, 123)
        self.assertEqual(f.result(), '123')
