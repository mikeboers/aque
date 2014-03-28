import os
import logging
import subprocess


log = logging.getLogger(__name__)


def do_shell_task(broker, task):

    cmd = task['args']
    kwargs = task.get('kwargs') or {}

    stdin_content = kwargs.pop('stdin', None)
    for name, mode in (
        ('stdin', 'r'),
        ('stdout', 'w'),
        ('stderr', 'w')
    ):
        key = name + "_path"
        if key in kwargs:
            kwargs[name] = open(kwargs.pop(key), mode)

    if stdin_content:
        kwargs['stdin'] = subprocess.PIPE

    env = os.environ.copy()
    env.update(kwargs.pop('env', {}))
    env['AQUE_TID'] = str(task['id'])
    kwargs['env'] = env
    
    proc = subprocess.Popen(cmd, **kwargs)
    if stdin_content:
        proc.stdin.write(stdin_content)
        proc.stdin.close()

    code = proc.wait()

    if code:
        broker.mark_as_error(task['id'], subprocess.CalledProcessError(code, cmd))
    else:
        broker.mark_as_success(task['id'], 0)

