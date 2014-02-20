from abc import ABCMeta, abstractmethod


class Broker(object):
    """Handles all negotiation between the client (e.g. Python) and server."""

    __metaclass__ = ABCMeta


    ## Low-level API

    @abstractmethod
    def get(self, tid, key, default=None):
        """Get a single value for a task ID and key."""

    @abstractmethod
    def getall(self, tid):
        """Get all data for a task ID.

        :returns dict: all task data.
        """

    @abstractmethod
    def set(self, tid, key, value):
        """Set a single value for a task ID and key."""

    @abstractmethod
    def setmany(self, tid, data):
        """Update a task's data."""

    ## Medium-level API

    @abstractmethod
    def new_task_id(self):
        """Get a new ID for a task."""

    ## High-level API

    @abstractmethod
    def mark_as_pending(self, tid):
        """Schedule a task to run."""

    @abstractmethod
    def mark_as_complete(self, tid, result):
        """Store a result and set the status to "complete"."""

    @abstractmethod
    def mark_as_error(self, tid, exc):
        """Store an error and set the status to "error"."""

    @abstractmethod
    def get_pending_task_ids(self):
        """Get a list of IDs of all top-level pending tasks."""

