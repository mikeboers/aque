"""aque xargs - xargs-like submitter of multiple tasks.

Submits multiple tasks with the same base command, taking the rest of the
arguments from stdin.

"""

from __future__ import division

import argparse
import itertools
import os
import sys
import shlex

import psutil

from aque.commands.main import main, command, argument, group
from aque import utils


def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    args = [iter(iterable)] * n
    return itertools.izip_longest(fillvalue=fillvalue, *args)

def tokenize_lines(count):
    for lines in grouper(sys.stdin, count):
        tokens = []
        for line in lines:
            tokens.extend(shlex.split(line))
        yield tokens

def tokenize_all():
    return [itertools.chain.from_iterable(shlex.split(line) for line in sys.stdin)]

def tokenize_words(count):
    return grouper(tokenize_all()[0], count)


@command(

    group('xargs compatibility',
        argument('-L', '--lines', type=int, metavar='N', help='''how many lines of input to use
            for arguments of a single task (for compatibility with `xargs -L`)'''),
        argument('-n', '--words', type=int, metavar='N', help='''how many works of input to use
            for arguments of a single task (for compatibility with `xargs -n`)'''),
        argument('-P', '--maxprocs', type=int, metavar='N', help='''how many tasks to run at once
            (for compatibility with `xargs -P` and exclusive with --cpus)'''),
    ),


    argument('-s', '--shell', action='store_true', help='''the first argument is
        executed as a shell script, with the rest provided to it as arguments'''),
    argument('-w', '--watch', action='store_true', help='watch the stdout/stderr of the task as it executes'),

    argument('--name', help='the task\'s name (for `aque status`)'),
    argument('-p', '--priority', type=int, help='higher ones go first'),

    argument('-c', '--cpus', type=int, help='how many CPUs to use per task'),
    argument('--host', help='the host(s) to run on'),
    argument('--platform', help='the platform to run on'),

    argument('-v', '--verbose', action='store_true', help='print IDs of all tasks'),
    argument('command', nargs=argparse.REMAINDER),
    help='xargs-like submitter of multiple tasks',
    description=__doc__,
)
def xargs(args):

    ids = []

    if args.lines:
        token_iter = tokenize_lines(args.lines)
    elif args.words:
        token_iter = tokenize_words(args.words)
    else:
        token_iter = tokenize_all()

    if args.cpus:
        cpus = args.cpus
    elif args.maxprocs:
        cpus = psutil.cpu_count() / args.maxprocs
    else:
        cpus = None


    options = {'environ': os.environ}
    for k in ('cwd', 'host', 'platform', 'priority'):
        v = getattr(args, k, None)
        if v is not None:
            options[k] = getattr(args, k)

    prototypes = []
    for tokens in token_iter:

        cmd = list(args.command)
        if args.shell:
            cmd.insert(0, os.environ.get('SHELL', '/bin/bash'))
            cmd.insert(1, '-c')
            cmd.insert(3, 'aque-submit')

        cmd.extend(t for t in tokens if t is not None)

        prototype = options.copy()
        prototype.update(
            pattern='shell',
            args=cmd,
            cpus=cpus,
            name=' '.join(cmd),

            # Magic prioritization!
            io_paths=utils.paths_from_args(cmd),
        )

        prototypes.append(prototype)

    future_map = args.queue.submit_many(prototypes)
    if args.verbose:
        print '\n'.join(str(tid) for tid in sorted(f.id for f in future_map.itervalues()))

    future = args.queue.submit_ex(
        pattern=None,
        name=args.name or 'xargs ' + ' '.join(args.command),
        dependencies=future_map.values(),
    )

    if args.watch:
        args = ['output', '--watch']
        args.extend(str(f.id) for f in future_map.itervalues())
        args.append(str(future.id))
        return main(args)

    print future.id
