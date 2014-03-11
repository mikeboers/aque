import sys

from aque.commands.main import command, argument
from aque.queue import Queue


@command(
    argument('-e', '--error', action='store_true'),
    argument('-s', '--success', action='store_true'),
    argument('-c', '--complete', action='store_true'),
    argument('-a', '--all', action='store_true'),
    # argument('-n', '--dry-run', action='store_true'),
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

    for status in statuses:
        filter_ = {}
        if status:
            filter_['status'] = status
        for task in args.broker.iter_tasks(**filter_):
            args.broker.delete(task['id'])
    for tid in args.id:
        args.broker.delete(tid)
