import sys

from aque.commands.main import command, argument


@command(
    help='task status',
    aliases=['ps'],
)
def status(args):
    for task in args.broker.iter_tasks():

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

        print '{id:5d} {status:7s} {pattern:7s} {func_spec}'.format(func_spec=func_spec, **task)
