import os
import sys

from aque.commands.main import command, argument
from aque.queue import Queue


@command(
    argument('-e', '--error', action='store_true'),
    argument('-s', '--success', action='store_true'),
    argument('-c', '--complete', action='store_true'),
    argument('-a', '--all', action='store_true'),
    argument('-x', '--all-users', action='store_true'),
    argument('id', nargs='*'),
    help='remove tasks',
)
def rm(args):

    statuses = set()
    if args.error:
        statuses.add('error')
    if args.success:
        statuses.add('success')
    if args.complete:
        statuses.add('error')
        statuses.add('success')
    if args.all:
        statuses = (None, )

    if not statuses and not args.id:
        exit(1)

    base_filter = {}
    if not args.all_users:
        base_filter['user'] = os.getlogin()

    to_delete = [int(x) for x in args.id]
    for status in statuses:
        filter_ = base_filter.copy()
        if status:
            filter_['status'] = status
        for task in args.broker.search(filter_, ['id']):
            if args.verbose:
                print task['id']
            to_delete.append(task['id'])

    args.broker.delete(to_delete)
    print 'removed', len(to_delete), 'tasks'
