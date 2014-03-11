import sys

from aque.commands.main import command, argument
from aque.worker import Worker


@command(
    argument('-1', '--one', action='store_true'),
    argument('-2', '--to-end', action='store_true'),
    help='run a worker',
)
def worker(args):
    worker = Worker(args.broker)
    if args.one:
        worker.run_one()
    elif args.to_end:
        worker.run_to_end()
    else:
        worker.run_forever()