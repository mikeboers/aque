import grp
import itertools
import os
import pwd

from redis import Redis
from aque.task import Task


class Queue(object):

    def __init__(self, redis=None, name='aque'):

        self.redis = redis or Redis()
        self.name = name

        self._dbid = self.redis.connection_pool.connection_kwargs['db']

    def format_key(self, format, *args, **kwargs):
        if kwargs.pop('_db', None):
            return ('{}@{}:' + format).format(self.name, self._dbid, *args)
        else:
            return ('{}:' + format).format(self.name, *args)

    def submit(self, task):
        task = self._submit(task)
        self.redis.rpush(self.format_key('pending_tasks'), task.id)
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
            id_num = self.redis.incr(self.format_key('task_counter'))
            task.id = self.format_key('task:{}', id_num)


        for subtask in itertools.chain(task.dependencies, task.children):
            self._submit(subtask)

        task.queue = self
        task.save()

        return task

    def load_task(self, tid):
        return Task._thaw(self, tid, self.redis.hgetall(tid))


