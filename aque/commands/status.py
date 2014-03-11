import sys

from aque.commands.main import command, argument


@command(
    help='task status',
    aliases=['ps'],
)
def status(args):
    tasks = list(args.broker.iter_tasks())
    tasks.sort(key=lambda t: t['id'])
    for task in tasks:

        arg_specs = []
        for arg in (task.get('args') or ()):
            arg_specs.append(repr(arg))
        for k, v in sorted((task.get('kwargs') or {}).iteritems()):
            arg_specs.append("%s=%r" % (k, v))
        func = task.get('func')
        if func:
            func_name = '%s.%s' % (func.__module__, func.__name__)
        else:
            func_name = ''
        func_spec = '%s(%s)' % (func_name, ', '.join(arg_specs))

        parts = [
            '%5d' % task['id'],
            '%7s' % task['status'],
            '%7s' % (task['pattern'] or '-'),
        ]
        if task.get('name'):
            parts.append(repr(task['name']))
        parts.append(func_spec)

        print ' '.join(parts)
