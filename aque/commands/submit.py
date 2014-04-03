import argparse
import os
import sys

from aque.commands.main import main, command, argument


@command(
    argument('--cwd'),
    argument('-i', '--stdin'),
    argument('--stdin-path'),
    argument('--stdout-path'),
    argument('--stderr-path'),
    argument('-q', '--quiet', action='store_true'),
    argument('-w', '--watch', action='store_true'),
    argument('-t', '--timeout', type=float),
    argument('command', nargs=argparse.REMAINDER),
    help='schedule a shell command',
    aliases=['s', 'sub'],
)
def submit(args):

    cmd = args.command
    kwargs = {
        'cwd': os.getcwd()
    }

    for k in ('stdin_path', 'stdout_path', 'stderr_path', 'stdin', 'cwd'):
        v = getattr(args, k, None)
        if v is not None:
            kwargs[k] = getattr(args, k)

    if args.verbose:
        print >> sys.stderr, cmd, kwargs

    future = args.queue.submit_ex(pattern='shell', args=cmd, kwargs=kwargs)

    if args.watch:
        return main(['output', '--watch', str(future.id)])

    if not args.quiet:
        print future.id
