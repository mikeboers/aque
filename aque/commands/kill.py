import os
import signal
import sys

from aque.commands.main import command, argument
from aque.queue import Queue


@command(
    argument('-s', '--signal', default='SIGKILL'),
    argument('tid', nargs='+', type=int),
    help='remove tasks',
)
def kill(args):

    # From the worker's perspective, and signal means that the task is dead.
    args.broker.set_status_and_notify(args.tid, 'killed', None)

    try:
        sig = getattr(signal, args.signal, None) or int(args.signal)
    except ValueError:
        print>>sys.stderr, 'unknown signal', args.signal
        return 1

    # Actually send across 
    args.broker.trigger(['signal_task.%d' % tid for tid in args.tid], args.tid, sig)
