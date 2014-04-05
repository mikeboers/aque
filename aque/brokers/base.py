from abc import ABCMeta, abstractmethod
import functools
import logging
import weakref

from aque.eventloop import EventLoop
from aque.futures import Future
from aque.utils import encode_callable


log = logging.getLogger(__name__)


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
        self._futures = weakref.WeakValueDictionary()
        self._bound_callbacks = {}

        # The event loop is set after the bind so that the bind does not
        # trigger the event loop to actually start. As of writing, that is
        # on the broker's themselves to manage.
        self._event_loop = None
        self.bind('task_status', self._on_task_status)
        self._event_loop = EventLoop()

    # BROKER API

    def after_fork(self):
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

    def create(self, prototypes):
        """Create a list of tasks, and return their :class:`.Future`s."""
        if isinstance(prototypes, dict):
            return self._create_many([prototypes])[0]
        else:
            return self._create_many(prototypes)

    @abstractmethod
    def _create_many(self, prototypes):
        pass

    def fetch(self, tids, fields=None):
        """Get the data for a given task IDs, with at least the given fields.

        :param list tids: task IDs to fetch.
        :param list fields: which fields the return tasks should have.
        :returns dict: mapping existing IDs to their tasks.
        """
        if isinstance(tids, int):
            try:
                return self._fetch_many([tids], fields)[tids]
            except KeyError:
                raise ValueError('unknown task ID %s' % tids)
        else:
            return self._fetch_many(tids, fields)

    @abstractmethod
    def _fetch_many(self, tids, fields):
        pass

    def delete(self, tids):
        if isinstance(tids, int):
            self._delete_many([tids])
        else:
            self._delete_many(tids)

    @abstractmethod
    def _delete_many(self, tids):
        pass
    

    # MID-LEVEL API

    def acquire(self, tid):
        """Capture a single task, and keep a heartbeat running."""
        return True

    def release(self, tid):
        """Release a single task that we are done with."""

    def bind(self, events, callback=None):
        """Schedule a function to be called whenever a given event happens."""
        if callback is None:
            return functools.partial(self.bind, events)
        events = [events] if isinstance(events, basestring) else list(events)
        for e in events:
            self._bound_callbacks.setdefault(e, []).append(callback)

    def unbind(self, events, callback):
        """Unschedule a function from a given event."""
        events = [events] if isinstance(events, basestring) else list(events)
        for e in events:
            self._bound_callbacks[e].remove(callback)
    
    def trigger(self, events, *args, **kwargs):
        """Trigger a set of events; scheduled functions will be called with given args."""
        events = [events] if isinstance(events, basestring) else list(events)
        self._dispatch_local_events(events, args, kwargs)
        self._send_remote_events(events, args, kwargs)

    def _dispatch_local_events(self, events, args, kwargs):
        for event in events:
            for callback in self._bound_callbacks.get(event, ()):
                try:
                    log.debug('dispatching %s to 0x%x %s(*%r, **%r)' % (event, id(callback), encode_callable(callback), args, kwargs))
                    callback(*args, **kwargs)
                except StandardError:
                    log.exception('error during event callback')

    def _send_remote_events(self, events, args, kwargs):
        pass

    # HIGH-LEVEL TASK API

    def get_future(self, tid):
        """Get a Future for a given task ID.

        Should return the same instance multiple times for the same task.
        """
        return self._futures.setdefault(tid, Future(self, tid))

    def set_status_and_notify(self, tids, status, result=None):
        if status not in ('pending', 'killed', 'success', 'error'):
            raise ValueError('bad status %r' % status)
        tids = [tids] if isinstance(tids, int) else list(tids)
        self._set_status(tids, status, result)
        events = ['task_status', 'task_status.%s' % status] + ['task_status.%d' % tid for tid in tids]
        self.trigger(events, tids, status)

    def _on_task_status(self, tids, status):

        if status not in ('success', 'error'):
            return

        futures = [self._futures.get(tid) for tid in tids]
        futures = filter(None, futures)
        if not futures:
            return

        tasks = self._fetch_many(tids, ['id', 'status', 'result'])

        for future in futures:

            task = tasks.get(future.id)
            if not task:
                log.warning('could not find task %d during task_status' % future.id)
                continue

            if task['status'] != status:
                log.warning('task %d status changed from %s to %s during event dispatch' % (task['id'], status, task['status']))

            log.debug('dispatching %s to %s' % (status, future.id))
            if status == 'success':
                future.set_result(task['result'])
            else:
                future.set_exception(task['result'])

    @abstractmethod
    def _set_status(self, tids, status, result):
        pass

    def log_output_and_notify(self, tid, fd, offset, content):
        self._log_output(tid, fd, offset, content)
        self.trigger(['output_log', 'output_log.%d' % tid], tid, fd, offset, content)

    def _log_output(self, tid, fd, content):
        pass

    def get_output(self, tids):
        return []

    @abstractmethod
    def search(self, filter=None, fields=None):
        """Get all tasks (restricted to the given fields).

        E.g.::
            broker.search({'status': 'pending'}, ['id', 'status', 'depedencies'])
            # returns iterator of pending tasks

        """


