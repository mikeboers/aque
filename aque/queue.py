import functools
import grp
import itertools
import logging
import os
import pwd

from aque.brokers import get_broker
from aque.exceptions import DependencyResolutionError
from aque.futures import Future
from aque.task import Task
from aque.utils import encode_callable

from redis import Redis


log = logging.getLogger(__name__)


_default_user = pwd.getpwuid(os.getuid())
_default_group = grp.getgrgid(_default_user.pw_gid)


class Queue(object):

    def __init__(self, broker=None):
        self.broker = get_broker(broker)

    def task(func=None, **options):
        if func is None:
            return funtools.partial(self.task, **options)
        return Task(func, self, options)

    def _set_defaults(self, task, parent={}):
        task['status']  = 'creating'
        task['pattern'] = encode_callable(task.get('pattern', 'generic'))
        task['func']    = encode_callable(task.get('func'))
        task['args']    = tuple(task.get('args') or ())
        task['kwargs']  = dict(task.get('kwargs') or {})
        task.setdefault('cwd'     , str(parent.get('cwd', os.getcwd())))
        task.setdefault('user'    , str(parent.get('user', _default_user.pw_name)))
        task.setdefault('group'   , str(parent.get('group', _default_group.gr_name)))
        task.setdefault('priority', int(parent.get('priority', 1000)))

    def submit(self, func, *args, **kwargs):
        return self.submit_ex(func, args, kwargs)

    def submit_ex(self, func=None, args=None, kwargs=None, **prototype):
        prototype['func'] = func
        prototype['args'] = args or ()
        prototype['kwargs'] = kwargs or {}
        return self.submit_many([prototype])[id(prototype)]

    def submit_many(self, prototypes):

        # First, we must flatten out the list of prototypes, stripping
        # dependencies but keeping track of them.
        to_process = list(self._flatten_prototypes(prototypes))

        futures_by_id = {}
        while to_process:

            # Resolve as many futures as possible.
            for proto in to_process:
                proto['dependencies'] = [x if isinstance(x, Future) else futures_by_id.get(x, x) for x in proto['dependencies']]

            # Sort it into those which are submittable, and those which are not.
            satisfied = []
            unsatisfied = []
            for proto in to_process:
                if all(isinstance(x, Future) for x in proto['dependencies']):
                    satisfied.append(proto)
                else:
                    unsatisfied.append(proto)
            to_process = unsatisfied

            if not satisfied:
                for i, proto in enumerate(to_process):
                    log.debug('%d/%d %r' % (i + 1, len(to_process), proto))
                for id_, future in sorted(futures_by_id.iteritems()):
                    log.debug('%d -> %d' % (id_, future.id))
                raise RuntimeError('could not satisfy any prototypes')

            for proto in satisfied:
                proto['dependencies'] = [f.id for f in proto['dependencies']]

            futures = self.broker.create(satisfied)
            for p, f in zip(satisfied, futures):
                futures_by_id[id(p)] = f

        self.broker.set_status_and_notify([f.id for f in futures_by_id.itervalues()], 'pending')

        return futures_by_id

    def _flatten_prototypes(self, prototypes, parent={}, recursion_tracker=None):

        recursion_tracker = {} if recursion_tracker is None else recursion_tracker

        for proto in prototypes:
            if not isinstance(proto, dict):
                raise TypeError('task prototypes must be dicts')

            self._set_defaults(proto, parent)
            pid = id(proto)

            # Need to track for dependency cycles (via a None signalling that
            # we are still processing this prototype, or a list signalling
            # that we are done with it.
            if pid in recursion_tracker:
                if recursion_tracker[pid]:
                    raise DependencyResolutionError('dependency cycle')
                else:
                    continue
            recursion_tracker[pid] = True

            deps = []
            for dep in proto.get('dependencies', ()):
                if isinstance(dep, dict):
                    deps.append(id(dep))
                    for x in self._flatten_prototypes([dep], proto, recursion_tracker):
                        yield x
                else:
                    deps.append(dep if isinstance(dep, Future) else self.broker.get_future(dep))

            recursion_tracker[pid] = False
            proto['dependencies'] = deps
            yield proto

