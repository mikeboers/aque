import itertools
import pprint
import time
import traceback
import threading

from aque.utils import decode_callable, encode_if_required, decode_if_possible
import aque.patterns
from .futures import Future



class Worker(object):

    def __init__(self, broker):
        self.broker = broker
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
                print 'end of queue (or stopped); sleeping...'
                if self._stopper.wait(1):
                    print 'stop requested'
                    return
            except KeyboardInterrupt:
                return
            except Exception:
                traceback.print_exc()

    def capture_task(self):
        for task in self.iter_open_tasks():

            # TODO: actually capture it
            return task

        return None

    def iter_open_tasks(self):

        tasks = list(self.broker.iter_pending_tasks())
        tasks.sort(key=lambda task: (task.get('priority', 1000), task['id']), reverse=True)

        considered = set()
        while tasks:

            task = tasks.pop(0)
            if task['id'] in considered:
                continue
            considered.add(task['id'])

            # TODO: make sure someone isn't working on it already.

            dep_tids = list(task.get('dependencies', ())) + list(task.get('children', ()))
            deps = [self.broker.get_data(dep_tid) for dep_tid in dep_tids]

            if any(dep['status'] != 'complete' for dep in deps):
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
        pattern_func = decode_callable(pattern_func)

        if pattern_func is None:
            raise TaskError('unknown pattern %r' % pattern_name)
        
        try:
            pattern_func(self.broker, task['id'], task)
        except Exception as e:
            self.broker.mark_as_error(task['id'], e)
            raise
        

        task = self.broker.get_data(task['id'])
        if task['status'] == 'complete':
            return task['result']
        elif task['status'] == 'error':
            raise task['exception']
        else:
            raise TaskIncomplete('task %r has incomplete status %r' % (task['id'], task['status']))










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
