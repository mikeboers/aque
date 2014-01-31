import logging

from aque.utils import decode_callable


log = logging.getLogger(__name__)


def do_reduce_task(task):

    func = decode_callable(task.func)

    args = task.args or ()
    if len(args) >= 2:
        task.error('too many args; reduce expects 1, got %d' % len(args))
        return

    sequence = [child.result() for child in task.children]

    # log.debug('reducing %r with %r and %r' % (sequence, func, args))
    
    task.complete(reduce(func, sequence, *args))
