import logging

from aque.utils import decode_callable


log = logging.getLogger(__name__)


def reduce_children(task):

    func = decode_callable(task.get('func'))

    args = task.get('args', ())
    if len(args) >= 2:
        task.error('too many args; reduce expects 1, got %d' % len(args))
        return

    sequence = [child.result() for child in task.children()]

    log.debug('reducing %r with %r and %r' % (sequence, func, args))
    
    task.complete(reduce(func, sequence, *args))
