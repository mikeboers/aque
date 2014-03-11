import sys

from aque.commands.main import command, argument


@command(
    argument('-w', '--wait', action='store_true'),
    argument('-t', '--timeout', type=float),
    argument('command', nargs='+'),
    help='schedule a shell command',
    aliases=['s', 'sub'],
)
def submit(args):
    cmd = args.command
    if args.verbose:
        print >> sys.stderr, cmd
    future = args.queue.submit_ex(pattern='shell', args=cmd)
    if args.wait:
        try:
            res = future.result(args.timeout)
        except Exception as e:
            print e.__class__.__name__, e
        else:
            print repr(res)
    else:
        print future.id
