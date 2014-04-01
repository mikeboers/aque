import threading

import aque.utils as utils
from .base import Broker


class MemoryBroker(Broker):
    """A :class:`.Broker` which holds everything in memory."""

    can_fork = False
    
    def __init__(self):
        super(MemoryBroker, self).__init__()
        self._id_lock = threading.Lock()
        self._init()

    def _init(self):
        self._tasks = {}
        self._id_counter = 0

    def update_schema(self):
        pass
    
    def destroy_schema(self):
        self._init()

    def create(self, prototype=None):
        return self.create_many([prototype])[0]

    def create_many(self, prototypes):
        futures = []
        with self._id_lock:
            for proto in prototypes:
                self._id_counter += 1
                tid = self._id_counter
                self._tasks[tid] = dict(proto or {})
                self._tasks[tid]['id'] = tid
                futures.append(self.get_future(tid))
        return futures

    def fetch(self, tid):
        return self._tasks[tid]

    def update(self, tid, data):
        self._tasks.setdefault(tid, {}).update(data)

    def delete(self, tid):
        self._tasks.pop(tid, None)

    def set_status_and_notify(self, tids, status):
        if not isinstance(tids, (tuple, list)):
            tids = [tids]
        for tid in tids:
            self.update(tid, {'status': status})

    def mark_as_success(self, tid, result):
        self.update(tid, {'status': 'success', 'result': result})
        future = self.futures.get(tid)
        if future:
            future.set_result(result)

    def mark_as_error(self, tid, exception):
        self.update(tid, {'status': 'error', 'result': exception})
        future = self.futures.get(tid)
        if future:
            future.set_exception(exception)

    def iter_tasks(self, **kwargs):
        for task in self._tasks.itervalues():
            for k, v in kwargs.iteritems():
                if task.get(k) != v:
                    continue
            yield task.copy()

