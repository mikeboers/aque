import os
import urlparse
from pprint import pprint
from unittest import TestCase

import psycopg2 as pg2

from aque import Queue, Future, execute
from aque.exceptions import DependencyError, TaskIncomplete, TaskError
from aque.worker import Worker
from aque.brokers import get_broker


class BrokerTestCase(TestCase):

    def setUp(self):
        self.name = 'aque' + self.__class__.__name__.title()

        self.broker = get_broker()
        self.broker.clear()
        self.broker.init()

        self.queue = Queue(self.broker)
        self.worker = Worker(self.broker)


class WorkerTestCase(BrokerTestCase):

    def setUp(self):
        super(WorkerTestCase, self).setUp()
        self.worker_thread = threading.Thread(target=worker.run_forever)
        self.worker_thread.start()

    def tearDown(self):
        self.worker.stop()
        self.worker_thread.join(1.1) # Just longer than the worker sleep time.
