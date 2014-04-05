"""aque rm - Remove tasks from the queue.

Removes given tasks, or tasks matching given statuses. By default only removes
tasks submitted by the current user, but may operate on all tasks via `-x`.

"""

import os
import sys

from aque.commands.main import command, argument
from aque.queue import Queue


@command(

    argument('-e', '--error',    action='store_true', help='remove all tasks which errored'),
    argument('-s', '--success',  action='store_true', help='remove all tasks which succeeded'),
    argument('-k', '--killed',   action='store_true', help='remove all tasks which were killed'),
    argument('-p', '--pending',  action='store_true', help='remove all tasks which are pending'),

    argument('-c', '--complete', action='store_true', help='remove all tasks which completed (same as `-esk`)'),

    argument('-a', '--all', action='store_true', help='remove all tasks (same as `-eskp`)'),
    argument('-x', '--all-users', action='store_true', help='affect tasks of other users as well'),
    argument('-v', '--verbose', action='store_true'),

    argument('tids', nargs='*', metavar='TID', help='specific task(s) to remove'),
    help='remove tasks from the queue',
    description=__doc__,
)
def rm(args):

    statuses = set()
    if args.pending:
        statuses.add('pending')
    if args.error:
        statuses.add('error')
    if args.success:
        statuses.add('success')
    if args.killed:
        statuses.add('killed')
    if args.complete:
        statuses.add('error')
        statuses.add('success')
        statuses.add('killed')
    if args.all:
        statuses = (None, )

    if not statuses and not args.tids:
        exit(1)

    base_filter = {}
    if not args.all_users:
        base_filter['user'] = os.getlogin()

    to_delete = [int(x) for x in args.tids]
    for status in statuses:
        filter_ = base_filter.copy()
        if status:
            filter_['status'] = status
        for task in args.broker.search(filter_, ['id']):
            if args.verbose:
                print task['id']
            to_delete.append(task['id'])

    args.broker.delete(to_delete)
    
    if not args.verbose:
        print 'removed', len(to_delete), 'tasks'
