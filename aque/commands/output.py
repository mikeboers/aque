import csv
import os
import sys

from aque.commands.main import command, argument


@command(
    argument('-r', '--recursive', action='store_true'),
    argument('tids', nargs='+', type=int),
    help='task output',
)
def output(args):
    for tid, ctime, fd, content in args.broker.get_output(args.tids):
        print tid, ctime, fd, content.encode('string-escape')
