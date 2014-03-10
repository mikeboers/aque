from aque.exceptions import DependencyError, TaskIncomplete, TaskError
from aque.futures import Future
from aque.patterns import pattern
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
