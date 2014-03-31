from __future__ import division

import itertools
import sys
import shlex

import psutil

from aque.commands.main import command, argument


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
    argument('-L', '--lines', type=int),
    argument('-n', '--words', type=int),
    argument('-P', '--maxprocs', type=int),
    argument('-c', '--cpus', type=int),
    argument('command', nargs='+'),
    help='schedule a series of commands like xargs',
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

    for tokens in token_iter:
        cmd = list(args.command)
        cmd.extend(t for t in tokens if t is not None)
        f = args.queue.submit_ex(pattern='shell', args=cmd, cpus=cpus)
        if args.verbose:
            print f.id
        ids.append(f.id)

    future = args.queue.submit_ex(pattern=None, dependencies=ids)
    print future.id
