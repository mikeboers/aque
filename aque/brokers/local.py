import threading

import aque.utils as utils
from .base import Broker


class LocalBroker(Broker):
    """A :class:`.Broker` which holds everything in memory."""

    def __init__(self):
        super(LocalBroker, self).__init__()
        self._id_lock = threading.Lock()
        self._id_counter = 0
        self._tasks = {}

    def create(self, prototype=None):
        with self._id_lock:
            self._id_counter += 1
            tid = self._id_counter
        self._tasks[tid] = dict(prototype or {})
        self._tasks[tid]['id'] = tid
        return self.get_future(tid)

    def fetch(self, tid):
        return self._tasks[tid]

    def update(self, tid, data):
        self._tasks.setdefault(tid, {}).update(data)

    def set_status_and_notify(self, tid, status):
        self.update(tid, {'status': status})

    def iter_pending_tasks(self):
        for task in self._tasks.itervalues():
            if task['status'] == 'pending':
                yield task.copy()

