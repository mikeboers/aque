import csv
import sys

from aque.commands.main import command, argument


@command(
    argument('tid', nargs='*'),
    argument('-f', '--csv'),
    help='task status',
    aliases=['ps'],
)
def status(args):

    if args.tid:
        tasks = [args.broker.fetch(tid) for tid in args.tid]
    else:
        tasks = list(args.broker.iter_tasks())
        tasks.sort(key=lambda t: t['id'])

    if args.csv:
        fields = [f.strip() for f in args.csv.split(',')]
        writer = csv.writer(sys.stdout)
        writer.writerow(fields)

    for task in tasks:

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

        parts = [
            '%5d' % task['id'],
            '%7s' % task['status'],
            '%7s' % (task['pattern'] or '-'),
        ]
        if task.get('name'):
            parts.append(repr(task['name']))
        parts.append(func_spec)
        if task['status'] in ('success', 'error'):
            parts.append('-> %r' % (task['result'], ))

        print ' '.join(parts)
