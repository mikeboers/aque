"""aque status - List tasks in the queue and their status.

Lists all tasks in the queue (limited to the current user by default) and
their status, arguments, or any other fields.

All of the fields of the standard task prototype are availible to `--filter`,
`--csv`, and `--pattern`, in addition to the following computed values:

    func_name: an entrypoints-style name of the function
    args_string: arguments and kwargs as they would be passed to a function
    func_signature: a representation of the called function and arguments
    running_time: a `datetime.timedelta` or None of the running time

"""

import csv
import os
import sys

from aque.commands.main import command, argument


@command(
    argument('-x', '--all-users', action='store_true', help='display tasks of all users'),
    argument('-f', '--filter', help='''Python expression determining if a given task should be
        displayed, e.g. `status in ('success', 'error')`'''),
    argument('-c', '--csv', help='comma-separated list of fields to output as a CSV'),
    argument('-p', '--pattern',
        default='{id:5d} {user:9s} {status:7s} {pattern:7s} "{name}" {func_signature} -> {result!r}',
        help='`str.format()` pattern for formatting each task'),
    argument('tids', nargs='*', type=int, metavar='TID', help='specific tasks to display'),
    help='list tasks in the queue and their status',
    description=__doc__,
)
def status(args):

    if args.tids:
        tasks = args.broker.fetch(args.tids).values()
    else:
        filter_ = {}
        if not args.all_users:
            filter_['user'] = os.getlogin()
        tasks = list(args.broker.search(filter_))

    tasks.sort(key=lambda t: t['id'])

    if args.csv:
        fields = [f.strip() for f in args.csv.split(',')]
        writer = csv.writer(sys.stdout)
        writer.writerow(fields)

    if args.filter:
        filter_ = compile(args.filter, '<--filter>', 'eval')
    else:
        filter_ = None

    for task in tasks:
        
        arg_specs = []
        for arg in (task.get('args') or ()):
            arg_specs.append(repr(arg))
        for k, v in sorted((task.get('kwargs') or {}).iteritems()):
            arg_specs.append("%s=%r" % (k, v))

        func = task.get('func')
        try:
            func_name = '%s:%s' % (func.__module__, func.__name__)
        except AttributeError:
            func_name = str(func or '')

        func_spec = '%s(%s)' % (func_name, ', '.join(arg_specs))

        task['func_name'] = func_name
        task['func_signature'] = func_spec
        task['args_string'] = ', '.join(arg_specs)
        task['pattern'] = task['pattern'] or '-'
        task['name'] = task['name'] or ''

        if task.get('first_active') is not None and task.get('last_active') is not None:
            task['running_time'] = task['last_active'] - task['first_active']
        else:
            task['running_time'] = None

        # This isn't normally good form, but since the implementation of this
        # thing allows you to do bad stuff, this isn't an added security risk.
        if filter_ and not eval(filter_, {}, task):
            continue

        if args.csv:
            data = [str(task.get(f)) for f in fields]
            writer.writerow(data)
        else:
            print args.pattern.format(**task)
