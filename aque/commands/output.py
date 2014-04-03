import csv
import os
import sys
from Queue import Queue

from aque.commands.main import command, argument


@command(
    argument('-r', '--recursive', action='store_true'),
    argument('-w', '--watch', action='store_true'),
    argument('tids', nargs='+', type=int),
    help='task output',
)
def output(args):

    if args.watch:

        queue = Queue()
        watching = set(args.tids)

        @args.broker.bind(['output_log.%d' % x for x in args.tids])
        def on_log(fd, content):
            queue.put((fd, content))

        @args.broker.bind(['task_status.%s' % x for x in args.tids])
        def on_status(tids, status):
            if status in ('success', 'error'):
                for x in tids:
                    watching.remove(x)
                    queue.put(None)

    for tid, ctime, fd, content in args.broker.get_output(args.tids):
        sys.stdout.write(content)
        sys.stdout.flush()

    if not any(t['status'] == 'pending' for t in args.broker.fetch(args.tids).itervalues()):
        return

    if args.watch:
        while watching:
            event_args = queue.get()
            if event_args is None:
                continue
            fd, content = event_args
            sys.stdout.write(content)
            sys.stdout.flush()



