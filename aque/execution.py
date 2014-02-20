import collections
import grp
import itertools
import logging
import os
import pkg_resources
import pwd

from aque.utils import decode_callable, encode_if_required, decode_if_possible
import aque.patterns
from .brokers import LocalBroker
from .futures import Future


log = logging.getLogger(__name__)


class DependencyError(RuntimeError):
    """Raised when task dependencies cannot be resolved."""

class TaskIncomplete(RuntimeError):
    """Raised by :meth:`Task.result` when the task did not complete."""

class TaskError(RuntimeError):
    """Raised by :meth:`Task.result` when the task errored without raiding an exception."""



def linearize_prototype(proto):
    """Return a list of task prototypes ordered so that dependencies come first.

    Output values are the input values, just re-organized.

    :raises DependencyError: when there is a loop.

    """
    return list(_iter_linearized(proto, set()))


def _iter_linearized(proto, visited):

    # HACK: we assume that dependencies and children are task prototypes,
    # and don't allow for instance IDs or futures.

    id_ = id(proto)
    if (id_, 'incomplete') in visited:
        raise DependencyError('dependency cycle')
    if id_ in visited:
        return

    visited.add((id_, 'incomplete'))
    visited.add(id_)

    for x in proto.get('dependencies', ()):
        for y in _iter_linearized(x, visited):
            yield y
    for x in proto.get('children', ()):
        for y in _iter_linearized(x, visited):
            yield y

    visited.remove((id_, 'incomplete'))
    yield proto


def execute(task):

    broker = LocalBroker()

    id_to_tid = {}
    def proto_tid(proto):
        if isinstance(proto, Future):
            return proto.id
        elif isinstance(proto, dict):
            return id_to_tid[id(proto)]
        elif isinstance(proto, basestring):
            return proto
        else:
            raise ValueError('expected prototype, Future, or task ID; got %r' % proto)

    for task in linearize_prototype(task):

        tid = broker.new_task_id()
        id_to_tid[id(task)] = tid

        # We need to modify the prototype to point at IDs for previous tasks.
        copy = dict(task)
        copy['dependencies'] = [proto_tid(t) for t in task.get('dependencies', ())]
        copy['children'] = [proto_tid(t) for t in task.get('children', ())]

        res = execute_one(broker, tid, copy)

    return res


def execute_one(broker, tid, task):
    """Find the pattern handler, call it, and catch errors.

    "dependencies" and "children" of the task MUST be a sequence of IDs.

    """

    pattern_name = task.get('pattern', 'generic')
    pattern_func = aque.patterns.registry.get(pattern_name, pattern_name)
    pattern_func = decode_callable(pattern_func)

    if pattern_func is None:
        raise TaskError('unknown pattern %r' % pattern_name)
    
    try:
        pattern_func(broker, tid, task)
    except Exception as e:
        broker.mark_as_error(tid, e)
        raise
    
    status = broker.get(tid, 'status')
    if status == 'complete':
        return broker.get(tid, 'result')
    elif status == 'error':
        raise broker.get(tid, 'exception')
    else:
        raise TaskIncomplete('incomplete status %r' % status)


