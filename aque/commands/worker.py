"""aque worker - Run a worker which processes the queue.

A worker is responsible for actually running the tasks on the queue, and for
mananging the CPU, memory, and other resources availible on the machine.

"""

import logging
import signal
import sys

from aque.commands.main import command, argument
from aque.worker import Worker


@command(
    argument('-1', '--one', action='store_true', help='run only a single task'),
    argument('-2', '--to-end', action='store_true', help='run only until there is nothing pending on the queue'),
    argument('-c', '--cpus', type=int, metavar='CPU_COUNT', help='how many CPUs to use'),
    help='run a worker',
    description=__doc__,
)
def worker(args):

    def on_hup(signum, frame):
        logging.getLogger(__name__).info('HUP! Stopping worker from taking more work.')
        worker.stop()
    signal.signal(signal.SIGHUP, on_hup)

    worker = Worker(args.broker, max_cpus=args.cpus)
    try:
        if args.one:
            worker.run_one()
        elif args.to_end:
            worker.run_to_end()
        else:
            worker.run_forever()
    except KeyboardInterrupt:
        pass
