from abc import ABCMeta, abstractmethod

from aque.futures import Future


class Broker(object):
    """Brokers handle all communication between clients and workers.

    The broker is responsible for storing and retreiving data (and any
    serialization required therein), and for orchestrating notifications
    between all parties.

    The API is as minimal as possible towards that goal."""

    __metaclass__ = ABCMeta

    def __init__(self):
        self.futures = {}

    @abstractmethod
    def create_task(self, prototype):
        """Create a task, and return a Future."""

    @abstractmethod
    def get_data(self, tid):
        """Get the data for a given task ID.

        ``retval['id']`` MUST be ``tid``.
        """

    def get_future(self, tid):
        """Get a Future for a given task ID.

        Should return the same instance multiple times for the same task.
        """
        return self.futures.setdefault(tid, Future(self, tid))

    ## High-level API

    def _update(self, tid, data):
        raise NotImplementedError()

    def _set_status_and_notify(self, tid, status):
        raise NotImplementedError()

    def mark_as_pending(self, tid, top_level=True):
        """Schedule a task to run."""
        self._set_status_and_notify(tid, 'pending')

    def mark_as_complete(self, tid, result):
        """Store a result and set the status to "complete"."""
        self._update(tid, {'result': result})
        self._set_status_and_notify(tid, 'complete')
        future = self.futures.get(tid)
        if future:
            future.set_result(result)

    def mark_as_error(self, tid, exc):
        """Store an error and set the status to "error"."""
        self._update(tid, {'exception': exc})
        self._set_status_and_notify(tid, 'pending')
        future = self.futures.get(tid)
        if future:
            future.set_exception(exc)

    @abstractmethod
    def iter_pending_tasks(self):
        """Get a list of IDs of all top-level pending tasks."""

