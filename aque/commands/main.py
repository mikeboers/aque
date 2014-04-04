import argparse
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


def command(*args, **kwargs):
    def _decorator(func):
        func.__aque_command__ = (args, kwargs)
        return func
    return _decorator


def main(argv=None):

    parser = argparse.ArgumentParser()
    parser.register('action', 'parsers', AliasedSubParsersAction)
    subparsers = parser.add_subparsers(help='sub-commands')

    parser.add_argument('--broker', dest='broker_url', default=os.environ.get('AQUE_BROKER'))
    parser.add_argument('-v', '--verbose', action='store_true')

    funcs = [ep.load() for ep in pkg_resources.iter_entry_points('aque_commands')]
    funcs.sort(key=lambda f: f.__aque_command__[1].get('name', f.__name__))

    for func in funcs:
        args, kwargs = func.__aque_command__
        subparser = subparsers.add_parser(
            kwargs.get('name', func.__name__),
            help=kwargs.get('help'),
            aliases=kwargs.get('aliases', []),
        )
        subparser.set_defaults(func=func)
        for arg_args, arg_kwargs in args:
            subparser.add_argument(*arg_args, **arg_kwargs)

    args = parser.parse_args(argv)
    if not args.func:
        parser.print_usage()
        exit(1)

    args.broker = get_broker(args.broker_url)
    args.queue = Queue(args.broker)
    
    try:
        res = args.func(args) or 0
    finally:
        args.broker.close()

    if __name__ == '__main__':
        exit(res)
    else:
        return res


