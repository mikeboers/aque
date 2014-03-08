import functools
import grp
import itertools
import os
import pwd

from aque.brokers.redis import RedisBroker
from aque.exceptions import DependencyError
from aque.futures import Future
from aque.task import Task

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

    def task(func=None, **options):
        if func is None:
            return funtools.partial(self.task, **options)
        return Task(func, self, options)

    def submit(self, func, *args, **kwargs):
        return self.submit_ex(func, args, kwargs)

    def submit_ex(self, func=None, args=None, kwargs=None, **prototype):
        prototype['func'] = func
        prototype['args'] = args or ()
        prototype['kwargs'] = kwargs or {}
        return self._submit(prototype, {}, {})

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
        task['status'] = 'pending'
        task.setdefault('pattern', 'generic')
        task.setdefault('user'    , parent.get('user', _default_user.pw_name))
        task.setdefault('group'   , parent.get('group', _default_group.gr_name))
        task.setdefault('priority', parent.get('priority', 1000))

        dep_futures = list(self._submit_dependencies(task, futures))
        task['dependencies'] = [f.id for f in dep_futures]
        
        future = self.broker.create(task)
        self.broker.mark_as_pending(future.id)

        futures[id_] = future
        return future

    def _submit_dependencies(self, parent, futures):
        for task in parent.get('dependencies', ()):
            if isinstance(task, Future):
                yield task
            elif isinstance(task, dict):
                yield self._submit(task, parent, futures)
            else:
                yield self.broker.get_future(task)



