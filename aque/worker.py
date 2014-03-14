import itertools
import pprint
import time
import traceback
import threading
import select
import multiprocessing
import sys
import os

from aque.brokers import get_broker
from aque.exceptions import TaskIncomplete
from aque.futures import Future
from aque.utils import decode_callable, encode_if_required, decode_if_possible
import aque.patterns


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
        self._execute(task)
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
            deps = self.broker.fetch_many(dep_ids).values()
            if any(dep['status'] != 'success' for dep in deps):
                tasks.extend(deps)
                continue

            if task['status'] == 'pending':
                yield task

    def _execute(self, task):
        """Find the pattern handler, call it, and catch errors.

        "dependencies" and "children" of the task MUST be a sequence of IDs.

        """

        pattern_name = task.get('pattern', 'generic')
        pattern_func = aque.patterns.registry.get(pattern_name, pattern_name)

        if pattern_func is None:
            self.broker.mark_as_success(task['id'], None)
            return

        pattern_func = decode_callable(pattern_func, 'aque_patterns')
        if pattern_func is None:
            raise TaskError('unknown pattern %r' % pattern_name)
        
        if self.broker.can_fork:
            out_r, out_w = os.pipe()
            err_r, err_w = os.pipe()
            thread = threading.Thread(target=self._watch_fds, args=(out_r, err_r))
            thread.daemon = True
            thread.start()
            proc = multiprocessing.Process(target=self._execute_subproc, args=(
                pattern_func, task, (out_w, err_w),
            ))
            proc.start()
            os.close(out_w)
            os.close(err_w)
            proc.join()
        else:
            self._execute_subproc(pattern_func, task)

    def _execute_subproc(self, func, task, fds=None):
        if fds:
            os.dup2(fds[0], 1)
            os.dup2(fds[1], 2)
        try:
            func(self.broker, task)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            self.broker.mark_as_error(task['id'], e)
            traceback.print_exc()
        finally:
            if fds:
                os.close(fds[0])
                os.close(fds[1])

    def _watch_fds(self, out, err):
        fds = [out, err]
        while fds:
            rfds, _, _ = select.select(fds, [], [], 1.0)
            for fd in rfds:
                out = os.read(fd, 8096)
                if out:
                    sys.stdout.write(out)
                    sys.stdout.flush()
                else:
                    fds.remove(fd)




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
