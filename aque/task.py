import aque.brokers.memory
import aque.queue
import aque.worker


class Task(object):

    def __init__(self, func, queue=None, options=None):
        self.func = func
        self.queue = queue
        self.options = options or {}
        functools.update_wrapper(self, func)

    def __call__(self, *args, **kwargs):
        return self.queue.submit_ex(self.func, args, kwargs, **self.options)

    def submit(self, *args, **kwargs):

        opts = self.options.copy()
        opts.update(kwargs)

        if opts.pop('local', False):
            broker = aque.brokers.memory.MemoryBroker()
            queue = aque.queue.Queue(broker=broker)
            future = queue.submit_ex(self.func, *args, **opts)
            worker = aque.worker.Worker(broker)
            worker.run_to_end()
            return future

        return self.queue.submit_ex(self.func, *args, **opts)