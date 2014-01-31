import logging
import subprocess


log = logging.getLogger(__name__)


def do_shell_task(task):

    args = task.args

    proc = subprocess.Popen(args)
    code = proc.wait()

    if code:
        task.error('exit code %d' % code)
    else:
        task.complete()

