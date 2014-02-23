import logging
import subprocess


log = logging.getLogger(__name__)


def do_shell_task(broker, tid, task):

    cmd = task['args']

    proc = subprocess.Popen(cmd)
    code = proc.wait()

    if code:
        broker.mark_as_error(tid, subprocess.CalledProcessError(code, cmd))
    else:
        broker.mark_as_complete(tid, 0)

