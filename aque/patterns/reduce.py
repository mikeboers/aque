import logging

from aque.utils import decode_callable


log = logging.getLogger(__name__)


def do_reduce_task(broker, task):

    func = decode_callable(task.get('func'))

    args = task.get('args') or ()
    if len(args) >= 2:
        task.error('too many args; reduce expects 1, got %d' % len(args))
        return

    sequence = [broker.fetch(cid).get('result') for cid in task.get('dependencies', ())]

    # log.debug('reducing %r with %r and %r' % (sequence, func, args))
    
    res = reduce(func, sequence, *args)
    broker.mark_as_success(task['id'], res)
