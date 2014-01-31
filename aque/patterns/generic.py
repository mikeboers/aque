import logging

from aque.utils import decode_callable


log = logging.getLogger(__name__)


def do_generic_task(task):

    func = decode_callable(task.func)
    args = task.args
    kwargs = task.kwargs

    # log.debug('calling %r with %r and %r' % (func, args, kwargs))
    task.complete(func(*args, **kwargs))
