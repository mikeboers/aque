from abc import ABCMeta, abstractmethod

from aque.futures import Future


class Broker(object):
    """Brokers handle all communication between clients and workers.

    The broker is responsible for storing and retreiving data (and any
    serialization required therein), and for orchestrating notifications
    between all parties.

    The API is as minimal as possible towards that goal."""

    __metaclass__ = ABCMeta

    @classmethod
    def from_url(cls, parts):
        """Construct a broker from the results of :func:`urlparse.urlsplit`."""
        return cls()

    def __init__(self):
        self.futures = {}

    def init(self):
        """Initialize the storage backing this broker.

        Normally only called by workers."""

    def clear(self):
        """Destroy all storage backing this broker.

        The broker is unusable unless :meth:`.init` is called."""

    @abstractmethod
    def create(self, prototype=None):
        """Create a task, and return a Future."""

    @abstractmethod
    def fetch(self, tid):
        """Get the data for a given task ID.

        ``retval['id']`` MUST be ``tid``.
        """

    @abstractmethod
    def update(self, tid, data):
        """Update the given task with the given data, but do NOT notify anyone.

        Generally used for finalizing the construction of tasks."""

    # High-level API

    def get_future(self, tid):
        """Get a Future for a given task ID.

        Should return the same instance multiple times for the same task.
        """
        return self.futures.setdefault(tid, Future(self, tid))

    def set_status_and_notify(self, tid, status):
        self.update(tid, {'status': status})

    def mark_as_pending(self, tid):
        """Schedule a task to run."""
        self.set_status_and_notify(tid, 'pending')

    @abstractmethod
    def mark_as_complete(self, tid, result):
        """Store a result and set the status to "complete"."""

    @abstractmethod
    def mark_as_error(self, tid, exc):
        """Store an error and set the status to "error"."""

    @abstractmethod
    def iter_pending_tasks(self):
        """Get a list of IDs of all top-level pending tasks."""

