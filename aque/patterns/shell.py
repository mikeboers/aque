import os
import logging
import signal
import subprocess


log = logging.getLogger(__name__)


def do_shell_task(task):

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
    
    proc = None
    def handler(signal, frame):
        log.info('caught signal %d; forwarding to proc' % signal)
        if proc:
            proc.send_signal(signal)

    # Forward signals to our subproc; can't catch SIGKILL, unfortunately.
    signals = (signal.SIGTERM, signal.SIGINT)
    handlers = [signal.signal(sig, handler) for sig in signals]

    try:
        proc = subprocess.Popen(cmd, **kwargs)
        if stdin_content:
            proc.stdin.write(stdin_content)
            proc.stdin.close()
        code = proc.wait()
        if code:
            raise subprocess.CalledProcessError(code, cmd)
        else:
            return 0

    finally:
        # Restore the signal handlers (just in case this isn't a forking broker).
        for sig, handler in zip(signals, handlers):
            signal.signal(sig, handler)

