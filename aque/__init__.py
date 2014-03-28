from aque.exceptions import DependencyResolutionError, DependencyFailedError
from aque.futures import Future
from aque.queue import Queue
from aque.brokers.memory import MemoryBroker
from aque.worker import Worker


def execute(task):

    broker = MemoryBroker()

    queue = Queue(broker=broker)
    future = queue.submit_ex(**task)

    worker = Worker(broker)
    worker.run_to_end()

    return future.result(timeout=0)
