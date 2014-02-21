import itertools
import pprint
import time
import traceback


from aque.execution import execute_one


class Worker(object):

    def __init__(self, broker):
        self.broker = broker

    def run_one(self):

        tid, task = self.capture_task()

        if not tid:
            return False

        print 'found new work:', tid
        print 'running...'
        try:
            res = execute_one(self.broker, tid, task)
        except:
            traceback.print_exc()
        else:
            print 'Results:',
            pprint.pprint(res)
            print '---'

        return True

    def run_to_end(self):
        while self.run_one():
            pass

    def run_forever(self):
        while True:
            try:
                if not self.run_one():
                    print 'no work found; sleeping...'
                    time.sleep(1)
            except KeyboardInterrupt:
                return

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
