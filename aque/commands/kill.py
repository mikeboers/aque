"""aque kill - Kill (or signal) tasks.

Marks a task's status as "killed" (preventing it from running), and sends
the given signal to any process that is currently running the task.

E.g.:

    $ aque kill -sINT 1234
    $ aque kill -s9 5678


"""

import os
import signal
import sys

from aque.commands.main import command, argument
from aque.queue import Queue


@command(
    argument('-s', '--signal', default='KILL', help='a signal accepted by kill(1) (default: 9 or KILL)'),
    argument('tid', nargs='+', type=int, help='the task(s) to kill/signal'),
    help='kill (or signal) tasks',
    description=__doc__,
)
def kill(args):

    # From the worker's perspective, and signal means that the task is dead.
    args.broker.set_status_and_notify(args.tid, 'killed', None)

    try:
        sig = getattr(signal, args.signal, None)
        if sig is None:
            sig = getattr(signal, 'SIG' + args.signal, None)
        if sig is None:
            int(args.signal)
    except ValueError:
        print>>sys.stderr, 'unknown signal', args.signal
        return 1

    # Actually send across 
    args.broker.trigger(['signal_task.%d' % tid for tid in args.tid], args.tid, sig)
