"""aque submit - Schedule a shell command.

Schedules to given command to run on the queue. The environment will have an
extra $AQUE_TID variable containing the ID of the running task.

E.g.:
    
    $ aque submit --shell 'echo $AQUE_TID says: "$@"' one two three

"""

import argparse
import os
import sys

from aque.commands.main import main, command, argument


@command(

    argument('--cwd', help='where to run the task (default: current directory)'),
    #argument('--stdin', help='path to read stdin from; "-" means this stdin (which is fully read before the task is submitted)'),
    #argument('--stdout', help='path to write stdout to'),
    #argument('--stderr', help='path to write stderr to'),

    argument('-s', '--shell', action='store_true', help='''the first argument is
        executed as a shell script, with the rest provided to it as arguments'''),
    argument('-w', '--watch', action='store_true', help='watch the stdout/stderr of the task as it executes'),

    argument('command', nargs=argparse.REMAINDER, metavar='COMMAND', help='the command to run'),
    help='schedule a shell command',
    description=__doc__,
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

    future = args.queue.submit_ex(pattern='shell', args=cmd, kwargs=kwargs)

    if args.watch:
        return main(['output', '--watch', str(future.id)])

    print future.id
