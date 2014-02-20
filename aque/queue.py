import grp
import itertools
import os
import pwd

from aque.brokers import RedisBroker
from aque.futures import Future

from redis import Redis


_default_user = pwd.getpwuid(os.getuid())
_default_group = grp.getgrgid(_default_user.pw_gid)


class Queue(object):

    def __init__(self, name='aque', hostname='localhost', port=6379, db=0, broker=None, redis=None):
        if not broker:
            if not redis:
                redis = Redis(hostname, port, db)
            broker = RedisBroker(name=name, redis=redis)
        self.broker = broker

    def submit(self, func, *args, **kwargs):
        return self.submit_ex(func, args, kwargs)

    def submit_ex(self, func=None, args=None, kwargs=None, task=None, **extra):

        task = dict(task or ())
        task.update(extra)

        task.setdefault('func', func)
        task.setdefault('args', args or ())
        task.setdefault('kwargs', kwargs or {})

        future = self._submit(task)
        self.broker.mark_as_pending(future.id)
        return future

    def _submit(self, task, parent={}, visited=None):

        visited = visited or set()
        if id(task) in visited:
            raise ValueError('recursive tasks')
        visited.add(id(task))

        task = dict(task)
        task.setdefault('pattern', 'generic')
        task.setdefault('user', parent.get('user', _default_user.pw_name))
        task.setdefault('group', parent.get('group', _default_group.gr_name))
        task.setdefault('priority', 1000)

        future = self.broker.get_future(self.broker.new_task_id())

        future.dependencies.extend(self._submit_dependencies(task, 'dependencies', visited))
        task['dependencies'] = [f.id for f in future.dependencies]
        future.children.extend(self._submit_dependencies(task, 'children', visited))
        task['children'] = [f.id for f in future.children]

        self.broker.setmany(future.id, task)

        return future

    def _submit_dependencies(self, task, key, visited):
        for subtask in task.get(key, ()):
            if isinstance(subtask, Future):
                yield subtask
            elif isinstance(subtask, dict):
                yield self._submit(subtask, task, visited)
            else:
                yield self.broker.get_future(subtask)

