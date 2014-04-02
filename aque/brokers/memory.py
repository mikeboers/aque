import logging
import threading

import aque.utils as utils
from .base import Broker


log = logging.getLogger(__name__)


class MemoryBroker(Broker):
    """A :class:`.Broker` which holds everything in memory."""

    can_fork = False
    
    def __init__(self):
        super(MemoryBroker, self).__init__()
        self._id_lock = threading.Lock()
        self._init()
        self._binds = {}

    def _init(self):
        self._tasks = {}
        self._id_counter = 0

    def update_schema(self):
        pass
    
    def destroy_schema(self):
        self._init()

    def _create_many(self, prototypes):
        futures = []
        with self._id_lock:
            for proto in prototypes:
                self._id_counter += 1
                tid = self._id_counter
                self._tasks[tid] = dict(proto or {})
                self._tasks[tid]['id'] = tid
                futures.append(self.get_future(tid))
        return futures

    def _fetch_many(self, tids, fields):
        res = {}
        for tid in tids:
            try:
                res[tid] = self._tasks[tid]
            except KeyError:
                pass
        return res

    def _delete_many(self, tids):
        for tid in tids:
            self._tasks.pop(tid, None)

    def _set_status(self, tids, status, result):
        for tid in tids:
            self._tasks.setdefault(tid, {}).update({'status': status, 'result': result})

    def search(self, filter=None, fields=None):
        filter_ = filter or {}
        for task in self._tasks.itervalues():
            for k, v in filter_.iteritems():
                if task.get(k) != v:
                    continue
            yield task.copy()

