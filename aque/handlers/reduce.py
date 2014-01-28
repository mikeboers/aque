import logging

from aque.utils import decode_callable


log = logging.getLogger(__name__)


def reduce_children(job):

    func = decode_callable(job.get('func'))

    args = job.get('args', ())
    if len(args) >= 2:
        job.error('too many args; reduce expects 1, got %d' % len(args))
        return

    sequence = [child.result() for child in job.children()]

    log.debug('reducing %r with %r and %r' % (sequence, func, args))
    
    job.complete(reduce(func, sequence, *args))
