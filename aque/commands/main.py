"""AQue is an asynchronous work queue, and a set of CLI and Python tools to use
and manage it.

See: `aque <command> --help` for more on individual commands.

"""

import argparse
import cProfile
import os
import pkg_resources

from aque.brokers import get_broker
from aque.queue import Queue


class AliasedSubParsersAction(argparse._SubParsersAction):
 
    def add_parser(self, name, **kwargs):
        aliases = kwargs.pop('aliases', [])
        parser = super(AliasedSubParsersAction, self).add_parser(name, **kwargs)
        for alias in aliases:
            pass # self._name_parser_map[alias] = parser
        return parser


def argument(*args, **kwargs):
    return args, kwargs

def group(title, *args):
    return title, args

def command(*args, **kwargs):
    def _decorator(func):
        func.__aque_command__ = (args, kwargs)
        return func
    return _decorator


def main(argv=None):

    parser = argparse.ArgumentParser(
        prog='aque',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=__doc__,
    )

    parser.register('action', 'parsers', AliasedSubParsersAction)
    subparsers = parser.add_subparsers(metavar='COMMAND')
    parser.add_argument('--broker',
        dest='broker_url',
        default=os.environ.get('AQUE_BROKER'),
        help='URL of broker to use (default: $AQUE_BROKER)',
    )

    parser.add_argument('--profile')

    funcs = [ep.load() for ep in pkg_resources.iter_entry_points('aque_commands')]
    funcs.sort(key=lambda f: f.__aque_command__[1].get('name', f.__name__))

    for func in funcs:
        args, kwargs = func.__aque_command__
        name = kwargs.pop('name', func.__name__)
        kwargs.setdefault('aliases', [])
        kwargs.setdefault('formatter_class', argparse.RawDescriptionHelpFormatter)
        subparser = subparsers.add_parser(name, **kwargs)
        subparser.set_defaults(func=func)

        for arg_args, arg_kwargs in args:
            if isinstance(arg_args, basestring):
                group = subparser.add_argument_group(arg_args)
                for arg_args, arg_kwargs in arg_kwargs:
                    group.add_argument(*arg_args, **arg_kwargs)
            else:
                subparser.add_argument(*arg_args, **arg_kwargs)

    args = parser.parse_args(argv)
    if not args.func:
        parser.print_usage()
        exit(1)

    args.broker = get_broker(args.broker_url)
    args.queue = Queue(args.broker)
    
    try:
        if args.profile:
            res = cProfile.runctx('args.func(args) or 0', globals(), locals(), args.profile)
        else:
            res = args.func(args) or 0
    finally:
        args.broker.close()

    if __name__ == '__main__':
        exit(res)
    else:
        return res


