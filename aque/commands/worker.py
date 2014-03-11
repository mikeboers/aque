import sys

from aque.commands.main import command, argument
from aque.worker import Worker


@command(
    help='run a worker',
)
def worker(args):
    worker = Worker(args.broker)
    worker.run_forever()
