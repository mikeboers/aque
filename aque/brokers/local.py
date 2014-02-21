
import aque.utils as utils
from .base import Broker


class LocalBroker(Broker):
    """A :class:`.Broker` which holds everything in memory."""

    def __init__(self):
        super(LocalBroker, self).__init__()
        self._id_counter = 0
        self._tasks = {}
        self._pending_tasks = []

    ## Low-level API

    def get(self, tid, key):
        return self._tasks.setdefault(tid, {}).get(key)

    def getall(self, tid):
        return dict(self._tasks.setdefault(tid, {}))

    def set(self, tid, key, value):
        self._tasks.setdefault(tid, {})[key] = value

    def setmany(self, tid, data):
        self._tasks.setdefault(tid, {}).update(data)

    ## Medium-level API

    def new_task_id(self):
        self._id_counter += 1
        return self._id_counter

    ## High-level API

    def mark_as_pending(self, tid):
        super(LocalBroker, self).mark_as_pending(tid)
        self._pending_tasks.append(tid)

    def mark_as_complete(self, tid, result):
        super(LocalBroker, self).mark_as_complete(tid, result)
        try:
            self._pending_tasks.remove(tid)
        except ValueError:
            pass

    def mark_as_error(self, tid, exc):
        """Store an error and set the status to "error"."""
        super(LocalBroker, self).mark_as_error(tid, exc)
        try:
            self._pending_tasks.remove(tid)
        except ValueError:
            pass

    def get_pending_task_ids(self):
        return list(self._pending_tasks)

