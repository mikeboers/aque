import itertools
import pprint
import time
import traceback


from aque.execution import execute_one


class Worker(object):

    def __init__(self, broker):
        self.broker = broker

    def run(self):
        while True:

            task = self.capture_task()
            if task:
                print 'found new work:', task
                print 'running...'
                try:
                    res = task._run()
                except:
                    traceback.print_exc()
                else:
                    print 'Results:',
                    pprint.pprint(res)
                    print '---'

                task.save()

            else:
                print 'no work found; sleeping...'
                time.sleep(1)

    def capture_task(self):
        return next(self.iter_open_tasks(), None)

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

            if any(self.broker.get(xid, 'status') != 'success' for xid in dep_ids):
                tasks.extend((xid, self.broker.getall(xid)) for xid in dep_ids)

            if self.broker.get(tid, 'status') == 'pending':
                yield task










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
