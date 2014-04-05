"""aque output -- Fetch (or watch) stdout/stderr of a task.

Fetches output of a task that has already run, and optionally waits for all
output yet to be generated. Watching terminates when the tasks complete.

"""

import csv
import os
import sys
from Queue import Queue

from aque.commands.main import command, argument


@command(
    argument('-w', '--watch', action='store_true', help='watch for more output until the task(s) terminate'),
    argument('tids', nargs='+', type=int, metavar='TID', help='ID(s) of the tasks to get output of'),
    help='fetch (or watch) stdout/stderr of a task',
    description=__doc__,
)
def output(args):

    if args.watch:

        watching = set(args.tids)
        queue = Queue()

        @args.broker.bind(['output_log.%d' % x for x in args.tids])
        def on_log(tid, fd, offset, content):
            queue.put((tid, fd, offset, content))

        @args.broker.bind(['task_status.%s' % x for x in args.tids])
        def on_status(tids, status):
            if status in ('success', 'error', 'killed'):
                for tid in tids:
                    queue.put((tid, None, None, None))

        found = args.broker.fetch(args.tids)
        watching.intersection_update(found)
        for task in found.itervalues():
            if task['status'] != 'pending':
                queue.put((task['id'], None, None, None))

        queue.put((None, None, None, None))

    max_offsets = {1: -1, 2: -1}

    for tid, ctime, fd, offset, content in args.broker.get_output(args.tids):
        stream = {1: sys.stdout, 2: sys.stderr}.get(fd)
        if stream:
            max_offsets[fd] = max(max_offsets[fd], offset)
            stream.write(content)
            stream.flush()

    if args.watch:
        while watching:

            tid, fd, offset, content = queue.get()
            if fd is None:
                try:
                    watching.remove(tid)
                except KeyError:
                    pass
                continue

            stream = {1: sys.stdout, 2: sys.stderr}.get(fd)
            if stream:

                if offset <= max_offsets[fd]:
                    continue
                else:
                    max_offsets[fd] = offset

                stream.write(content)
                stream.flush()



