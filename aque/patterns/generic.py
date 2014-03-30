import logging

from aque.utils import decode_callable


log = logging.getLogger(__name__)


def do_generic_task(task):

    func = decode_callable(task.get('func'))
    args = task.get('args') or ()
    kwargs = task.get('kwargs') or {}

    # log.debug('calling %r with %r and %r' % (func, args, kwargs))
    
    return func(*args, **kwargs)

