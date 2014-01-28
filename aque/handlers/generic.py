import logging

from aque.utils import decode_callable


log = logging.getLogger(__name__)


def handle_generic(job):

    func = decode_callable(job.get('func'))
    args = job.get('args', ())
    kwargs = job.get('kwargs', {})

    log.debug('calling %r with %r and %r' % (func, args, kwargs))
    
    job.complete(func(*args, **kwargs))
