import argparse
import os
import sys

from aque.commands.main import main, command, argument


@command(
    argument('--cwd'),
    argument('-w', '--watch', action='store_true'),
    argument('-t', '--timeout', type=float),
    argument('-s', '--shell', action='store_true'),
    argument('command', nargs=argparse.REMAINDER),
    help='schedule a shell command',
    aliases=['s', 'sub'],
)
def submit(args):

    cmd = list(args.command)
    if args.shell:
        cmd.insert(0, os.environ.get('SHELL', '/bin/bash'))
        cmd.insert(1, '-c')
        cmd.insert(3, 'aque-submit')

    kwargs = {
        'cwd': os.getcwd()
    }

    for k in ('cwd', ):
        v = getattr(args, k, None)
        if v is not None:
            kwargs[k] = getattr(args, k)

    if args.verbose:
        print >> sys.stderr, cmd, kwargs

    future = args.queue.submit_ex(pattern='shell', args=cmd, kwargs=kwargs)

    if args.watch:
        return main(['output', '--watch', str(future.id)])

    print future.id
