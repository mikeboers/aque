import csv
import os
import sys

from aque.commands.main import command, argument


@command(
    argument('-a', '--all', action='store_true'),
    argument('-f', '--filter'),
    argument('-c', '--csv'),
    argument('-p', '--pattern', default='{id:5d} {status:7s} {pattern:7s} "{name}" {func_signature} -> {result!r}'),
    argument('tid', nargs='*'),
    help='task status',
    aliases=['ps'],
)
def status(args):

    if args.tid:
        tasks = [args.broker.fetch(tid) for tid in args.tid]
    else:
        filter_ = {}
        if not args.all:
            filter_['user'] = os.getlogin()
        tasks = list(args.broker.iter_tasks(**filter_))
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

        if filter_ and not eval(filter_, {}, task):
            continue

        if args.csv:
            data = [str(task.get(f)) for f in fields]
            writer.writerow(data)
            continue
        
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

        print args.pattern.format(**task)
