import sys

from aque.commands.main import command, argument
from aque.worker import Worker


@command(
    argument('--reset', action='store_true', help='destroy the schema'),
    help='initialize a broker',
)
def init(args):
    if args.reset:
        args.broker.destroy_schema()
    args.broker.update_schema()
