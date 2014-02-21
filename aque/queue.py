import grp
import itertools
import os
import pwd

from aque.brokers import RedisBroker
from aque.futures import Future
from aque.execution import DependencyError

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

    def submit_ex(self, func=None, args=None, kwargs=None, **prototype):
        prototype.setdefault('func', func)
        prototype.setdefault('args', args or ())
        prototype.setdefault('kwargs', kwargs or {})
        return self.submit_prototype(prototype)

    def submit_prototype(self, prototype):
        future = self._submit(prototype, {}, {})
        self.broker.mark_as_pending(future.id)
        return future

    def _submit(self, task, parent, futures):

        # We need to linearize the submission. We pass around a mapping of
        # object ids to their futures. If the id is in the dict but maps to None
        # then we are in progress of creating that future, and there must be
        # a dependency error.
        id_ = id(task)
        try:
            future = futures[id_]
        except KeyError:
            pass
        else:
            if future:
                return future
            else:
                raise DependencyError('dependency cycle')

        futures[id_] = None

        task = dict(task)
        task.setdefault('pattern', 'generic')
        task.setdefault('user', parent.get('user', _default_user.pw_name))
        task.setdefault('group', parent.get('group', _default_group.gr_name))
        task.setdefault('priority', 1000)

        future = self.broker.get_future(self.broker.new_task_id())

        future.dependencies.extend(self._submit_dependencies(task, 'dependencies', futures))
        task['dependencies'] = [f.id for f in future.dependencies]
        future.children.extend(self._submit_dependencies(task, 'children', futures))
        task['children'] = [f.id for f in future.children]

        self.broker.setmany(future.id, task)
        
        futures[id_] = future

        return future

    def _submit_dependencies(self, task, key, futures):
        for subtask in task.get(key, ()):
            if isinstance(subtask, Future):
                yield subtask
            elif isinstance(subtask, dict):
                yield self._submit(subtask, task, futures)
            else:
                yield self.broker.get_future(subtask)

