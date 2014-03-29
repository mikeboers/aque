import contextlib
import itertools
import logging
import multiprocessing
import os
import pprint
import select
import sys
import threading
import time
import traceback

import psutil

from aque.brokers import get_broker
from aque.exceptions import DependencyFailedError, DependencyResolutionError, PatternIncompleteError, PatternMissingError
from aque.futures import Future
from aque.utils import decode_callable, encode_if_required, decode_if_possible, parse_bytes


log = logging.getLogger(__name__)


CPU_COUNT = psutil.cpu_count()
MEM_TOTAL = psutil.virtual_memory().total


_default_resources = {
    'cpu': 1,
    'mem': 0,
}

def sub_resources(a, b):
    for k, v in _default_resources.iteritems():
        b.setdefault(k, v)
    for k, v in b.iteritems():
        if k == 'mem' and isinstance(v, basestring):
            v = parse_bytes(v)
        a[k] = a.get(k, 0) - v
    return a


class Worker(object):

    def __init__(self, broker=None):
        self.broker = get_broker(broker)

        self._stopper = threading.Event()

        self._running = []
        self._finished_one = threading.Event()

    def stop(self):
        self._stopper.set()

    def __del__(self):
        self.stop()

    def _available_resources(self):
        res = {
            'cpu': CPU_COUNT,
            'mem': MEM_TOTAL,
        }
        for task in self._running:
            sub_resources(res, task.get('requirements') or {})
        return res

    def run_one(self):
        start_time = time.time()
        for task in self.iter_open_tasks():
            if self.broker.capture(task['id']):
                try:
                    exec_time = time.time()
                    self.execute(task)
                    return True
                finally:
                    end_time = time.time()
                    logging.debug('found task %d in %.3fms; ran in %.3fms' % (
                        task['id'],
                        1000 * (exec_time - start_time), 
                        1000 * (end_time - exec_time),
                    ))
                    self.broker.release(task['id'])

    def run_to_end(self):
        self._stopper.clear()
        while not self._stopper.is_set() and self.run_one():
            pass

    def run_forever(self):
        self._stopper.clear()
        while not self._stopper.is_set():
            if not self.run_one():
                time.sleep(0.5)

    def _run_in_thread(self, task):
        try:
            self.execute(task)
        finally:
            self.broker.release(task['id'])
            self._running.remove(task)
            self._finished_one.set()

    def iter_open_tasks(self):

        tasks = list(self.broker.iter_tasks(status='pending'))
        task_cache = dict((t['id'], t) for t in tasks)

        considered = set()
        while tasks:

            for task in tasks:
                if '_priority_key' not in task:
                    ### TODO: Scale by duration, relative IO speeds, etc..
                    task['_priority_key'] = (
                        -task.get('priority', 1000),
                        task['id'],
                    )
            tasks.sort(key=lambda task: task['_priority_key'])

            task = tasks.pop(0)
            if task['id'] in considered:
                continue
            considered.add(task['id'])

            # Just in case (this does actually happen with the MemoryBroker).
            if task['status'] != 'pending':
                continue

            dependency_ids = task.get('dependencies')

            # Cache the onces we haven't seen before.
            uncached = [tid for tid in dependency_ids if tid not in task_cache]
            if uncached:
                task_cache.update(self.broker.fetch_many(uncached))

            skip_task = False

            for tid in dependency_ids:

                dep = task_cache.get(tid)

                if not dep:
                    log.warning('task %r is missing dependency %r' % (task['id'], tid))
                    self.broker.mark_as_error(task['id'],
                        DependencyResolutionError('task %r does not exist' % tid),
                    )
                    skip_task = True
                    continue

                if dep['status'] == 'pending':
                    skip_task = True
                    tasks.append(dep)

                elif dep['status'] == 'paused':
                    skip_task = True

                elif dep['status'] != 'success':
                    skip_task = True
                    log.info('task %r has failed dependency %r' % (task['id'], tid))
                    self.broker.mark_as_error(task['id'],
                        DependencyFailedError('task %r has status %r' % (tid, dep['status']))
                    )

            if skip_task:
                continue

            yield task

    def execute(self, task):
        """Find the pattern handler, call it, and catch errors.

        "dependencies" and "children" of the task MUST be a sequence of IDs.

        """

        # Shortcut for grouping tasks.
        if task.get('pattern', 'xxx') is None:
            self.broker.mark_as_success(task['id'], None)
            return
        
        # If we are allowed, we will run the work in a subprocess.
        if self.broker.can_fork:

            # We create a new set of pipes for standard IO, so that we may
            # capture and/or manipulate them outselves.
            out_r, out_w = os.pipe()
            err_r, err_w = os.pipe()

            # Start the actuall subprocess.
            proc = multiprocessing.Process(target=self._forked_execute, args=(
                task, out_w, err_w,
            ))
            proc.start()

            # Close our copies of the write end of the pipes.
            os.close(out_w)
            os.close(err_w)

            # Wait for it to finish.
            self._watch_fds(out_r, err_r)
            proc.join()

        else:
            self._execute(task)

    def _forked_execute(self, task, out_fd, err_fd):

        self.broker.did_fork()

        # Prep the stdio; close stdin and redirect stdout/err to the parent's
        # preferred pipes. Everything should clean itself up.
        os.close(0)
        os.dup2(out_fd, 1)
        os.dup2(err_fd, 2)

        self._execute(task)

    def _watch_fds(self, out_fd, err_fd):
        fds = [out_fd, err_fd]
        redirections = {
            out_fd: sys.stdout,
            err_fd: sys.stderr,
        }
        while fds:
            rfds, _, _ = select.select(fds, [], [], 1.0)
            for fd in rfds:
                out = os.read(fd, 65536)
                if out:
                    fh = redirections[fd]
                    fh.write(out)
                    fh.flush()
                else:
                    os.close(fd)
                    fds.remove(fd)

    def _execute(self, task):
        try:

            encoded_pattern = task.get('pattern', 'generic')
            try:
                pattern_func = decode_callable(encoded_pattern, 'aque_patterns')
            except ValueError:
                pattern_func = None
            if pattern_func is None:
                raise PatternMissingError('cannot decode pattern from %r' % encoded_pattern)

            pattern_func(self.broker, task)

        except KeyboardInterrupt:
            raise
        
        except Exception as e:
            self.broker.mark_as_error(task['id'], e)
            return
            # traceback.print_exc()

        # Make sure that the pattern actually did something.
        # XXX: Surely I can just fetch one field...
        if self.broker.fetch(task['id'])['status'] == 'pending':
            self.broker.mark_as_error(task['id'], PatternIncompleteError('the pattern did not complete'))

