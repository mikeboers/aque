from cStringIO import StringIO
from csv import DictReader
from pprint import pprint
from subprocess import CalledProcessError
from unittest import TestCase as BaseTestCase
import contextlib
import datetime
import errno
import functools
import os
import re
import shutil
import sys
import threading
import time
import urlparse

import psycopg2 as pg2

from aque import execute
from aque.brokers import get_broker
from aque.commands.main import main
from aque.eventloop import SelectableEvent, EventLoop, StopSelection
from aque.exceptions import DependencyFailedError, DependencyResolutionError, PatternMissingError
from aque.futures import Future
from aque.local import current_task, current_broker
from aque.queue import Queue
from aque.worker import Worker


def sandbox(*args):
    path = os.path.abspath(os.path.join(
        __file__,
        '..',
        'sandbox',
        datetime.datetime.utcnow().isoformat('T'),
        *args
    ))
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
    return path


@contextlib.contextmanager
def override_stdio(stdout=True, stderr=False, stdin=None):

    real_in, real_out, real_err = sys.stdin, sys.stdout, sys.stderr
    if stdin is not None:
        sys.stdin = StringIO(stdin)
    if stdout:
        sys.stdout = stdout = StringIO()
    if stderr:
        sys.stderr = stderr = StringIO()
    try:
        yield stdout, stderr
    finally:
        if stdout:
            stdout.seek(0)
        if stderr:
            stderr.seek(0)
        sys.stdin, sys.stdout, sys.stderr = real_in, real_out, real_err


def self_call(args):
    res = main(args)
    if res:
        raise CalledProcessError(res)


def self_check_output(args, stdin=None):
    with override_stdio(stdin=stdin) as (out, _):
        main(args)
    return out.getvalue()


class TestCase(BaseTestCase):

    @property
    def sandbox(self):
        return sandbox(self.id())

    def assertSearch(self, pattern, content):
        if not re.search(pattern, content):
            self.fail('\'%s\' does not match %r' % (pattern, content))


class BrokerTestCase(TestCase):

    def setUp(self):
        self.name = 'aque' + self.__class__.__name__.title()
        self.broker = get_broker()
        if False:
            self.broker.clear()
        self.queue = Queue(self.broker)
        self.worker = Worker(self.broker)
        self.worker.run_to_end()

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
