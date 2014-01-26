import logging

from aque.utils import decode_callable


log = logging.getLogger(__name__)


def reduce_children(job):

    func = decode_callable(job.get('func'))
    sequence = [child.result() for child in job.children()]
    try:
        args = (job['initial'], )
    except:
        args = ()

    log.debug('reducing %r with %r and %r' % (sequence, func, args))
    
    job.success(reduce(func, sequence, *args))
