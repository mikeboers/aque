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

    can_fork = True
    
    @classmethod
    def from_url(cls, parts):
        """Construct a broker from a URL.

        :param url: either a ``str`` or results from :func:`urlparse.urlsplit`.
        :returns: :class:`Broker`

        """
        return cls()

    def __init__(self):
        self.futures = {}
        self._bound_callbacks = {}

    # BROKER API

    def did_fork(self):
        """Called by a :class:`Worker` after forking."""
    
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

    def fetch_many(self, tids):
        """Get the data for several tasks.

        Missing tasks will not be in the resultant dictionary.

        """
        res = {}
        for tid in tids:
            try:
                res[tid] = self.fetch(tid)
            except ValueError:
                pass
        return res

    @abstractmethod
    def update(self, tid, data):
        """Update the given task with the given data, but do NOT notify anyone.

        Generally used for finalizing the construction of tasks."""

    @abstractmethod
    def delete(self, tid):
        """Delete the given task."""
    
    # MID-LEVEL TASK API

    def acquire(self, tid):
        """Capture the task, and keep a heartbeat running."""
        return True

    def release(self, tid):
        """Release a task that we are done with."""

    # MID-lEVEL EVENT API

    def bind(self, event, callback):
        """Schedule a function to be called whenever a given event happens."""
        self._bound_callbacks.setdefault(event, []).append(callback)

    def unbind(self, event, callback):
        """Unschedule a function from a given event."""
        self._bound_callbacks[event].remove(callback)

    def _iter_bound_callbacks(self, event):
        return self._bound_callbacks.get(event, ())
    
    @abstractmethod
    def trigger(self, event, *args, **kwargs):
        """Trigger an event; scheduled functions will be called with given args."""

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

    def log_output(self, tid, fd, content):
        pass

    @abstractmethod
    def iter_tasks(self, **kwargs):
        """Get all tasks (restricted to the given fields).

        E.g.::
            broker.iter_tasks(status='pending')
            # returns iterator of pending tasks

        """

