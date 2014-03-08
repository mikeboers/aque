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
        self._top_level_pending_tasks = []

    def create_task(self, prototype):
        with self._id_lock:
            self._id_counter += 1
            tid = self._id_counter
        self._tasks[tid] = dict(prototype)
        self._tasks[tid]['id'] = tid
        return self.get_future(tid)

    def get_data(self, tid):
        return self._tasks[tid]

    def _update(self, tid, data):
        self._tasks.setdefault(tid, {}).update(data)

    def _set_status_and_notify(self, tid, status):
        self._tasks.setdefault(tid, {})['status'] = status

    def mark_as_pending(self, tid, top_level=True):
        super(LocalBroker, self).mark_as_pending(tid)
        if top_level:
            self._top_level_pending_tasks.append(tid)

    def mark_as_complete(self, tid, result):
        super(LocalBroker, self).mark_as_complete(tid, result)
        try:
            self._top_level_pending_tasks.remove(tid)
        except ValueError:
            pass

    def mark_as_error(self, tid, exc):
        """Store an error and set the status to "error"."""
        super(LocalBroker, self).mark_as_error(tid, exc)
        try:
            self._top_level_pending_tasks.remove(tid)
        except ValueError:
            pass

    def iter_pending_tasks(self):
        for tid in self._top_level_pending_tasks:
            yield self._tasks[tid].copy()

