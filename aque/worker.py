import itertools
import pprint
import time
import traceback
import threading
import select
import multiprocessing
import sys
import os
import logging

from aque.brokers import get_broker
from aque.exceptions import TaskIncomplete, PreconditionFailed, DependencyError
from aque.futures import Future
from aque.utils import decode_callable, encode_if_required, decode_if_possible


log = logging.getLogger(__name__)


class Worker(object):

    def __init__(self, broker=None):
        self.broker = get_broker(broker)
        self._stopper = threading.Event()

    def stop(self):
        self._stopper.set()

    def run_one(self):
        task = self.capture_task()
        if not task:
            return False
        self.execute(task)
        return True

    def run_to_end(self):
        self._stopper.clear()
        while not self._stopper.is_set() and self.run_one():
            pass

    def run_forever(self):
        self._stopper.clear()
        while True:
            try:
                self.run_to_end()
                print 'waiting for more work...'
                if self._stopper.wait(1):
                    return
            except KeyboardInterrupt:
                return
                

    def capture_task(self):
        for task in self.iter_open_tasks():

            # TODO: actually capture it
            return task

        return None

    def iter_open_tasks(self):

        tasks = list(self.broker.iter_tasks(status='pending'))

        considered = set()
        while tasks:

            tasks.sort(key=lambda task: (-task.get('priority', 1000), task['id']))

            task = tasks.pop(0)
            if task['id'] in considered:
                continue
            considered.add(task['id'])

            # TODO: make sure someone isn't working on it already.

            dep_ids = task.get('dependencies')
            deps = self.broker.fetch_many(dep_ids)
            for dep_id in dep_ids:
                dep = deps.get(dep_id)
                if not dep:
                    log.warning('task %r is missing dependency %r' % (task['id'], dep_id))
                    self.broker.mark_as_error(task['id'],
                        DependencyError('task %r does not exist' % dep_id),
                    )
                    break
                if dep['status'] not in ('pending', 'success'):
                    log.info('task %r has failed dependency %r' % (task['id'], dep_id))
                    self.broker.mark_as_error(task['id'],
                        PreconditionFailed('task %r has status %r' % (dep_id, dep['status']))
                    )
                    break

            if any(dep['status'] != 'success' for dep in deps.itervalues()):
                tasks.extend(deps.itervalues())
                continue

            if task['status'] == 'pending':
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

            # Lets watch them for logging.
            fd_thread = threading.Thread(target=self._watch_fds, args=(out_r, err_r))
            fd_thread.daemon = True
            fd_thread.start()

            # Start the actuall subprocess.
            proc = multiprocessing.Process(target=self._forked_execute, args=(
                task, out_w, err_w,
            ))
            proc.start()

            # Close our copies of the write end of the pipes.
            os.close(out_w)
            os.close(err_w)

            # Wait for it to finish.
            # TODO: add this to a pool and manage resources.
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
                    fds.remove(fd)

    def _execute(self, task):
        try:
            encoded_pattern = task.get('pattern', 'generic')
            pattern_func = decode_callable(encoded_pattern, 'aque_patterns')
            if pattern_func is None:
                raise TaskError('unknown pattern %r' % encoded_pattern)
            pattern_func(self.broker, task)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            self.broker.mark_as_error(task['id'], e)
            traceback.print_exc()




if __name__ == '__main__':

    import argparse

    from redis import Redis
    from aque.brokers.redis import RedisBroker
    

    parser = argparse.ArgumentParser()
    parser.add_argument('queue', default='aque')
    args = parser.parse_args()

    broker = RedisBroker(args.queue, Redis())
    worker = Worker(broker)
    worker.run_forever()
