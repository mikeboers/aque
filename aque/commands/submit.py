import sys

from aque.commands.main import command, argument
from aque.queue import Queue


@command(
    argument('command'),
    argument('args', nargs='*'),
    help='schedule a shell command',
    aliases=['s', 'sub'],
)
def submit(args):
    cmd = [args.command] + args.args
    if args.verbose:
        print >> sys.stderr, cmd
    future = args.queue.submit_ex(pattern='shell', args=cmd)
    print future.id
