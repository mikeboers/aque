import argparse
import os

from aque.brokers import get_broker
from aque.queue import Queue


class AliasedSubParsersAction(argparse._SubParsersAction):
 
    class _AliasedPseudoAction(argparse.Action):
        def __init__(self, name, aliases, help):
            dest = name
            if aliases:
                dest += ' (%s)' % ','.join(aliases)
            sup = super(AliasedSubParsersAction._AliasedPseudoAction, self)
            sup.__init__(option_strings=[], dest=dest, help=help) 
 
    def add_parser(self, name, **kwargs):
        if 'aliases' in kwargs:
            aliases = kwargs['aliases']
            del kwargs['aliases']
        else:
            aliases = []
 
        parser = super(AliasedSubParsersAction, self).add_parser(name, **kwargs)
 
        # Make the aliases work.
        for alias in aliases:
            self._name_parser_map[alias] = parser
        # Make the help text reflect them, first removing old help entry.
        if 'help' in kwargs:
            help = kwargs.pop('help')
            self._choices_actions.pop()
            pseudo_action = self._AliasedPseudoAction(name, aliases, help)
            self._choices_actions.append(pseudo_action)
 
        return parser


parser = argparse.ArgumentParser()
parser.register('action', 'parsers', AliasedSubParsersAction)
subparsers = parser.add_subparsers(help='sub-commands')

parser.add_argument('--broker', dest='broker_url', default=os.environ.get('AQUE_BROKER'))
parser.add_argument('-v', '--verbose', action='store_true')


def argument(*args, **kwargs):
    return args, kwargs


def command(*args, **kwargs):
    def _decorator(func):
        subparser = subparsers.add_parser(
            kwargs.pop('name', func.__name__),
            help=kwargs.pop('help', None),
            aliases=kwargs.pop('aliases', []),
        )
        subparser.set_defaults(func=func)
        for arg_args, arg_kwargs in args:
            subparser.add_argument(*arg_args, **arg_kwargs)
        return func
    return _decorator


# Register the commands.
# import aque.commands.init
# import aque.commands.kill
# import aque.commands.ps
# import aque.commands.rm
import aque.commands.submit
import aque.commands.worker
# import aque.commands.xargs


def main():

    args = parser.parse_args()
    if not args.func:
        parser.print_usage()
        exit(1)

    args.broker = get_broker(args.broker_url)
    args.queue = Queue(args.broker)
    exit(args.func(args) or 0)

