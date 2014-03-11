from abc import ABCMeta, abstractmethod

from aque.futures import Future


class Broker(object):
    """Brokers handle all communication between clients and workers.

    The broker is responsible for storing and retreiving data (and any
    serialization required therein), and for orchestrating notifications
    between all parties.

    The API is as minimal as possible towards that goal.

    """

    __metaclass__ = ABCMeta

    @classmethod
    def from_url(cls, parts):
        """Construct a broker from a URL.

        :param url: either a ``str`` or results from :func:`urlparse.urlsplit`.
        :returns: :class:`Broker`

        """
        return cls()

    def __init__(self):
        self.futures = {}

    # BROKER API

    def update_schema(self):
        """Assert that the backend schema exists, and is up to date."""

    def destroy_data(self):
        """Destroy all data.

        Depending on the broker, the instance may no longer be usable, and
        the underlying storage may require :meth:`update_schema` be called
        before reuse."""

    def close(self):
        """Closes all resources in use by the :class:`Broker`."""
    

    # LOW-LEVEL TASK API

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


    # HIGH-LEVEL TASK API

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
    def mark_as_success(self, tid, result):
        """Store a result and set the status to "success"."""

    @abstractmethod
    def mark_as_error(self, tid, exc):
        """Store an error and set the status to "error"."""

    @abstractmethod
    def iter_tasks(self, status=None):
        """Get all tasks (restricted to the given status)."""

