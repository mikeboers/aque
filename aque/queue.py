import grp
import itertools
import os
import pwd

from redis import Redis
from aque.task import Task
from aque.broker import Broker


class Queue(object):

    def __init__(self, name='aque', hostname='localhost', port=6379, db=0, broker=None, redis=None):
        if not broker:
            if not redis:
                redis = Redis(hostname, port, db)
            broker = Broker(name=name, redis=redis)
        self.broker = broker

    def submit(self, task):
        task = self._submit(task)
        self.broker.mark_as_pending(task.id)
        return task.id

    def _submit(self, task):

        if not isinstance(task, Task):
            if isinstance(task, dict):
                task = Task(**task)
            else:
                raise TypeError('not a Task')

        if task.status != 'pending':
            raise ValueError('task is not pending; got %r' % task.status)

        if task.id is None:
            task.id = self.broker.new_task_id()


        for subtask in itertools.chain(task.dependencies, task.children):
            self._submit(subtask)

        self.broker.save_task(task)

        return task
