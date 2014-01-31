import grp
import os
import pwd

from aque.task import Task


class Queue(object):

    def __init__(self, redis, name='aque'):

        self.redis = redis
        self.name = name

        self._dbid = redis.connection_pool.connection_kwargs['db']

    def _format(self, format, *args, **kwargs):
        if kwargs.pop('_db', None):
            return ('{}@{}:' + format).format(self.name, self._dbid, *args)
        else:
            return ('{}:' + format).format(self.name, *args)

    def submit(self, task):

        if not isinstance(task, Task):
            task = Task(task)

        user = pwd.getpwuid(os.getuid())
        group = grp.getgrgid(user.pw_gid)

        task._store.setdefault('priority', 1000)
        task._store.setdefault('user', user.pw_name)
        task._store.setdefault('group', group.gr_name)

        id_num = self.redis.incr(self._format('task_counter'))

        task.id = self._format('task:{}', id_num)
        task.status = 'pending'

        self.redis.hmset(task.id, task._store)
        self.redis.rpush(self._format('pending_tasks'), task.id)
        self.redis.publish(self._format('{}:status', id_num, _db=True), task.status)

        task.is_frozen = True
        return task.id

