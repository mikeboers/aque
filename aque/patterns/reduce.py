import logging

from aque import current_broker
from aque.utils import decode_callable


log = logging.getLogger(__name__)


def do_reduce_task(task):

    func = decode_callable(task.get('func'))

    args = task.get('args') or ()
    if len(args) >= 2:
        task.error('too many args; reduce expects 1, got %d' % len(args))
        return

    dependency_ids = task.get('dependencies') or []
    deps = current_broker().fetch(dependency_ids)
    sequence = [deps[id_]['result'] for id_ in dependency_ids]

    # log.debug('reducing %r with %r and %r' % (sequence, func, args))
    
    return reduce(func, sequence, *args)
