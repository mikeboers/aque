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
from aque import utils


@command(

    argument('--cwd', help='where to run the task (default: current directory)'),
    #argument('--stdin', help='path to read stdin from; "-" means this stdin (which is fully read before the task is submitted)'),
    #argument('--stdout', help='path to write stdout to'),
    #argument('--stderr', help='path to write stderr to'),

    argument('-n', '--name', help='the task\'s name (for `aque status`)'),
    argument('-p', '--priority', type=int, help='higher ones go first'),

    argument('-c', '--cpus', type=int, help='how many CPUs to use per task'),
    argument('--host', help='the host(s) to run on'),
    argument('--platform', help='the platform to run on'),

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

    options = {'environ': os.environ}

    for k in ('cpus', 'cwd', 'host', 'platform', 'priority'):
        v = getattr(args, k, None)
        if v is not None:
            options[k] = getattr(args, k)

    options.setdefault('io_paths', utils.paths_from_args(cmd))

    name = args.name or ' '.join(cmd)
    future = args.queue.submit_ex(pattern='shell', args=cmd, name=name, **options)

    if args.watch:
        return main(['output', '--watch', str(future.id)])

    print future.id
