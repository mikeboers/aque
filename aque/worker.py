import itertools
import pprint
import time
import traceback

from aque.utils import decode_callable, encode_if_required, decode_if_possible
import aque.patterns
from .futures import Future



class Worker(object):

    def __init__(self, broker):
        self.broker = broker

    def run_one(self):
        tid, task = self.capture_task()
        if not tid:
            return False
        self._execute(tid, task)
        return True

    def run_to_end(self):
        while self.run_one():
            pass

    def run_forever(self):
        while True:
            try:
                self.run_to_end()
                print 'end of queue; sleeping...'
                time.sleep(1)
            except KeyboardInterrupt:
                return
            except Exception:
                traceback.print_exc()

    def capture_task(self):
        return next(self.iter_open_tasks(), (None, None))

    def iter_open_tasks(self):

        task_ids = self.broker.get_pending_task_ids()
        tasks = [(tid, self.broker.getall(tid)) for tid in task_ids]
        tasks.sort(key=lambda (tid, task): (task.get('priority', 1000), tid), reverse=True)

        considered = set()
        while tasks:

            tid, task = tasks.pop(0)
            considered.add(tid)

            # TODO: make sure someone isn't working on it already.

            dep_ids = list(task.get('dependencies', ())) + list(task.get('children', ()))
            deps = []

            if any(self.broker.get(xid, 'status') != 'complete' for xid in dep_ids):
                tasks.extend((xid, self.broker.getall(xid)) for xid in dep_ids)
                continue

            status = self.broker.get(tid, 'status')
            if status in ('pending', None):
                yield tid, task

    def _execute(self, tid, task):
        """Find the pattern handler, call it, and catch errors.

        "dependencies" and "children" of the task MUST be a sequence of IDs.

        """

        pattern_name = task.get('pattern', 'generic')
        pattern_func = aque.patterns.registry.get(pattern_name, pattern_name)
        pattern_func = decode_callable(pattern_func)

        if pattern_func is None:
            raise TaskError('unknown pattern %r' % pattern_name)
        
        try:
            pattern_func(self.broker, tid, task)
        except Exception as e:
            broker.mark_as_error(tid, e)
            raise
        
        status = self.broker.get(tid, 'status')
        if status == 'complete':
            return self.broker.get(tid, 'result')
        elif status == 'error':
            raise self.broker.get(tid, 'exception')
        else:
            raise TaskIncomplete('incomplete status %r' % status)










if __name__ == '__main__':

    import argparse

    from redis import Redis
    from aque.queue import Queue


    parser = argparse.ArgumentParser()
    parser.add_argument('queue', default='aque')
    args = parser.parse_args()

    redis = Redis()
    queue = Queue(redis, args.queue)
    worker = Worker(queue)
    worker.run()
