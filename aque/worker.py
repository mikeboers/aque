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
from aque.exceptions import DependencyFailedError, DependencyResolutionError, PatternMissingError
from aque.futures import Future
from aque.local import _local
from aque.utils import decode_callable, encode_if_required, decode_if_possible, parse_bytes
from aque.eventloop import SelectableEvent, EventLoop, StopSelection


log = logging.getLogger(__name__)


CPU_COUNT = psutil.cpu_count()
MEM_TOTAL = psutil.virtual_memory().total


class BaseJob(object):

    def __init__(self, worker, task):

        self.worker = worker
        self.broker = worker.broker
        self.task = task
        self.id = task['id']

        self.finished = SelectableEvent()

    def close(self):
        self.finished.close()

    def start(self):
        pass

    def execute(self):
        try:

            encoded_pattern = self.task.get('pattern', 'generic')
            try:
                pattern_func = decode_callable(encoded_pattern, 'aque_patterns')
            except ValueError:
                pattern_func = None
            if pattern_func is None:
                raise PatternMissingError('cannot decode pattern from %r' % encoded_pattern)

            _local.task = self.task
            _local.broker = self.broker

            res = pattern_func(self.task)

        except KeyboardInterrupt:
            raise
        
        except Exception as e:
            self.broker.mark_as_error(self.id, e)

        else:
            self.broker.mark_as_success(self.id, res)

        finally:
            self.finished.set()


class ThreadJob(BaseJob):

    def start(self):
        self.thread = threading.Thread(target=self.execute)
        self.thread.start()

    def to_select(self):
        return [self.finished.fileno()], [], []

    def on_select(self, rfds, wfds, xfds):
        if not self.thread.is_alive() or self.finished.is_set():
            raise StopSelection()


class ProcJob(BaseJob):

    def start(self):

        # We create a new set of pipes for standard IO, so that we may
        # capture and/or manipulate them outselves.
        o_rfd, o_wfd = os.pipe()
        e_rfd, e_wfd = os.pipe()

        self.redirections = {
            o_rfd: sys.stdout,
            e_rfd: sys.stderr,
        }

        # Start the actuall subprocess.
        self.proc = multiprocessing.Process(target=self.target, args=(o_wfd, e_wfd))
        self.proc.start()

        log.log(5, 'proc %d for task %d started' % (self.proc.pid, self.id))

        # Close our copies of the write end of the pipes.
        os.close(o_wfd)
        os.close(e_wfd)

    def to_select(self):
        rfds = self.redirections.keys()
        rfds.append(self.finished.fileno())
        return rfds, [], []

    def on_select(self, rfds, wfds, xfds):

        for rfd in rfds:
            stream = self.redirections.get(rfd)
            if stream:
                x = os.read(rfd, 65536)
                if x:
                    stream.write(x)
                else:
                    os.close(rfd)
                    del self.redirections[rfd]

        if (not self.redirections or not rfds) and (not self.proc.is_alive() or self.finished.is_set()):
            log.log(5, 'proc %d for task %d joined' % (self.proc.pid, self.id))
            raise StopSelection()

    def target(self, o_wfd, e_wfd):

        self.broker.did_fork()

        # Prep the stdio; close stdin and redirect stdout/err to the parent's
        # preferred pipes. Everything should clean itself up.
        os.close(0)
        os.dup2(o_wfd, 1)
        os.dup2(e_wfd, 2)
        os.close(o_wfd)
        os.close(e_wfd)

        self.execute()


def task_cpus(task):
    cpus = task.get('cpus')
    if cpus is None:
        return 1.0
    else:
        return float(cpus)

def task_memory(task):
    memory = task.get('memory')
    return memory or 0


class Worker(object):

    def __init__(self, broker=None, max_cpus=None):
        self.broker = get_broker(broker)

        self._event_loop = EventLoop()
        self._stopper = threading.Event()

    def stop(self):
        self._stopper.set()

    def __del__(self):
        self.stop()

    def run_one(self):
        self._run(count=1, wait_for_more=False)

    def run_to_end(self):
        self._run(count=None, wait_for_more=False)

    def run_forever(self):
        self._run(count=None, wait_for_more=True)

    def _resources_left(self):
        cpus = CPU_COUNT
        memory = MEM_TOTAL
        for obj in self._event_loop.active:
            if isinstance(obj, BaseJob):
                cpus -= task_cpus(obj.task)
                memory -= task_memory(obj.task)
        return cpus, memory

    def _run(self, count, wait_for_more):

        self._stopper.clear()
        while not self._stopper.is_set():

            cpus, memory = self._resources_left()

            # Check that we haven't spawned too many yet AND there are enough
            # resources to spare.
            # TODO: wait until a reasonable amount of time has passed or we have
            #       notification that there are new jobs.
            task_iter = None
            while (count is None or count > 0) and cpus > 0 and memory > 0:

                task_iter = task_iter or self.iter_open_tasks()
                try:
                    task = next(task_iter)
                except StopIteration:
                    break

                # TODO: Make sure we can satisfy it.

                if not self.broker.capture(task['id']):
                    continue

                job = (ProcJob if self.broker.can_fork else ThreadJob)(self, task)
                job.start()
                self._event_loop.add(job)

                cpus, memory = self._resources_left()

                count = count - 1 if count is not None else None

            self._event_loop.process(timeout=1.0)

            # Deal with any jobs that just stopped.
            job_just_finished = False
            for obj in self._event_loop.stopped:
                if isinstance(obj, BaseJob):
                    obj.close()
                    self.broker.release(obj.id)
                    job_just_finished = True

            self._event_loop.stopped[:] = []

            if not job_just_finished and not any(isinstance(x, BaseJob) for x in self._event_loop.active):
                if wait_for_more:
                    print 'waiting for more work...'
                    time.sleep(1)
                else:
                    return

    def iter_open_tasks(self):

        pending_tasks = list(self.broker.iter_tasks(status='pending', fields=['id', 'status', 'dependencies']))

        task_cache = dict((t['id'], t) for t in pending_tasks)
        task_priorities = {}

        considered = set()
        while True:

            # Make sure we are only looking at pending tasks that we have not
            # already considered. (The pending check is required for the
            # MemoryBroker, which sometimes modifies tasks.)
            pending_tasks = [
                task for task in pending_tasks
                if task['status'] == 'pending' and task['id'] not in considered
            ]
            if not pending_tasks:
                break

            # Create dynamic priorities for every task, and sort by them.
            for task in pending_tasks:
                if task['id'] not in task_priorities:
                    ### TODO: Scale by duration, relative IO speeds, etc..
                    task_priorities[task['id']] = (
                        -task.get('priority', 1000),
                        task['id'],
                    )
            pending_tasks.sort(key=lambda task: task_priorities[task['id']])
            task = pending_tasks.pop(0)

            dependency_ids = task.get('dependencies') or []

            # Cache the ones we haven't seen before.
            uncached_ids = [tid for tid in dependency_ids if tid not in task_cache]
            if uncached_ids:
                task_cache.update(self.broker.fetch_many(uncached_ids, fields=['id', 'status', 'dependencies']))

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
                    pending_tasks.append(dep)

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

            yield self.broker.fetch(task['id'])

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
            pass

        else:
            self._execute(task)


