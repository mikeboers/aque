import itertools
import pprint
import time
import traceback


class Worker(object):

    def __init__(self, queue):
        self.queue = queue
        self.redis = queue.redis

    def run(self):
        while True:

            task = self.find_new_work()
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
                print 'no work found'
            
            time.sleep(1)


    def find_new_work(self):

        task_ids = set(self.redis.lrange(self.queue.format_key('pending_tasks'), 0, -1))

        tasks = [self.queue.load_task(id_) for id_ in task_ids]
        tasks.sort(key=lambda t: (t.priority, t.id), reverse=True)

        considered = set()
        while tasks:

            task = tasks.pop(0)
            considered.add(task)

            # TODO: make sure someone isn't working on it already.

            tasks.extend(task.dependencies)
            tasks.extend(task.children)

            if any(t.status != 'success' for t in itertools.chain(task.dependencies, task.children)):
                continue

            if task.status == 'pending':
                return task










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
