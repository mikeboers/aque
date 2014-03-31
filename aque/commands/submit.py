import argparse
import os
import sys

from aque.commands.main import command, argument


@command(
    argument('--cwd'),
    argument('-i', '--stdin'),
    argument('--stdin-path'),
    argument('--stdout-path'),
    argument('--stderr-path'),
    argument('-q', '--quiet', action='store_true'),
    argument('-w', '--wait', action='store_true'),
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
    if args.wait:
        try:
            res = future.result(args.timeout)
        except Exception as e:
            if not args.quiet:
                print e.__class__.__name__, e
            return 1
        else:
            if not args.quiet:
                print repr(res)
            return 0
    else:
        if not args.quiet:
            print future.id
