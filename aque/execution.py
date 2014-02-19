import collections
import grp
import itertools
import logging
import os
import pkg_resources
import pwd

from aque.utils import decode_callable, encode_if_required, decode_if_possible
import aque.patterns
from aque.brokers import LocalBroker


log = logging.getLogger(__name__)


class DependencyError(RuntimeError):
    """Raised when task dependencies cannot be resolved."""

class TaskIncomplete(RuntimeError):
    """Raised by :meth:`Task.result` when the task did not complete."""

class TaskError(RuntimeError):
    """Raised by :meth:`Task.result` when the task errored without raiding an exception."""



def iter_linearized(task, _visited=None):

    id_ = id(task)
    _visited = _visited or set()
    if (id_, 'incomplete') in _visited:
        raise DependencyError('dependency cycle')
    if id_ in _visited:
        return

    _visited.add((id_, 'incomplete'))
    _visited.add(id_)

    for x in task.get('dependencies', ()):
        for y in iter_linearized(x, _visited):
            yield y
    for x in task.get('children', ()):
        for y in iter_linearized(x, _visited):
            yield y

    _visited.remove((id_, 'incomplete'))
    yield task


def execute(task, broker=None):
    broker = broker or LocalBroker()
    for task in list(iter_linearized(task)):

        task['_id'] = tid = broker.new_task_id()

        copy = dict(task)
        copy['dependencies'] = [t['_id'] for t in task.get('dependencies', ())]
        copy['children'] = [t['_id'] for t in task.get('children', ())]

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
        raise TaskError('no aque pattern %r' % pattern_name)

    # log.debug('handling task %r with %r' % (self['id'], pattern))
    
    try:
        pattern_func(broker, tid, task)
    except Exception as e:
        broker.mark_as_error(tid, e)
        raise e
    
    status = broker.get(tid, 'status')
    if status == 'complete':
        return broker.get(tid, 'result')
    elif status == 'error':
        raise broker.get(tid, 'exception')
    else:
        raise TaskIncomplete('incomplete status %r' % status)


