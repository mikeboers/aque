import logging

from aque.utils import decode_callable


log = logging.getLogger(__name__)


def do_generic_task(broker, tid, task):

    func = decode_callable(task.get('func'))
    args = task.get('args') or ()
    kwargs = task.get('kwargs') or {}

    # log.debug('calling %r with %r and %r' % (func, args, kwargs))
    
    try:
        res = func(*args, **kwargs)
    except Exception as e:
        broker.mark_as_error(tid, e)
    else:
        broker.mark_as_complete(tid, res)

