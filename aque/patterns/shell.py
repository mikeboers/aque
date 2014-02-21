import logging
import subprocess


log = logging.getLogger(__name__)


def do_shell_task(broker, tid, task):

    args = task['args']

    proc = subprocess.Popen(args)
    code = proc.wait()

    if code:
        broker.mark_as_error(tid, ValueError(code))
    else:
        broker.mark_as_complete(tid, 0)

