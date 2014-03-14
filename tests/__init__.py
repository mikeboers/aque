from cStringIO import StringIO
from pprint import pprint
from subprocess import CalledProcessError
from unittest import TestCase
import contextlib
import os
import re
import sys
import threading
import urlparse
from csv import DictReader

import psycopg2 as pg2

from aque import Queue, Future, execute
from aque.brokers import get_broker
from aque.exceptions import DependencyError, TaskIncomplete, TaskError
from aque.worker import Worker
from aque.commands.main import main


@contextlib.contextmanager
def capture_output(out=True, err=False):
    real_out, real_err = sys.stdout, sys.stderr
    if out:
        sys.stdout = out = StringIO()
    if err:
        sys.stderr = err = StringIO()
    yield out, err
    if out:
        out.seek(0)
    if err:
        err.seek(0)
    sys.stdout, sys.stderr = real_out, real_err

def self_call(args):
    res = main(args)
    if res:
        raise CalledProcessError(res)

def self_check_output(args):
    with capture_output() as (out, _):
        main(args)
    return out.getvalue()


class BrokerTestCase(TestCase):

    def setUp(self):
        self.name = 'aque' + self.__class__.__name__.title()
        self.broker = get_broker()
        if False:
            self.broker.clear()
        self.queue = Queue(self.broker)
        self.worker = Worker(self.broker)

    def tearDown(self):
        self.broker.close()


class WorkerTestCase(BrokerTestCase):

    def setUp(self):
        super(WorkerTestCase, self).setUp()
        self.worker_thread = threading.Thread(target=self.worker.run_forever)
        self.worker_thread.start()

    def tearDown(self):
        super(WorkerTestCase, self).tearDown()
        self.worker.stop()
        self.worker_thread.join(1.1) # Just longer than the worker sleep time.
