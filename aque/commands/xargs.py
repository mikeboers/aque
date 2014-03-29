import itertools
import sys
import shlex
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

def tokenize_words(count):
    tokens = itertools.chain.from_iterable(shlex.split(line) for line in sys.stdin)
    return grouper(tokens, count)


@command(
    argument('-L', '--lines', type=int),
    argument('-n', '--words', type=int),
    argument('command', nargs='+'),
    help='schedule a series of commands like xargs',
    aliases=['s', 'sub'],
)
def xargs(args):

    ids = []

    if args.lines:
        token_iter = tokenize_lines(args.lines)
    else:
        token_iter = tokenize_words(args.words or 1)

    for tokens in token_iter:
        cmd = list(args.command)
        cmd.extend(t for t in tokens if t is not None)
        f = args.queue.submit_ex(pattern='shell', args=cmd)
        if args.verbose:
            print f.id
        ids.append(f.id)

    future = args.queue.submit_ex(pattern=None, dependencies=ids)
    print future.id
