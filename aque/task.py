import collections
import itertools
import logging
import pkg_resources

from aque.utils import decode_callable
import aque.patterns


log = logging.getLogger(__name__)


class DependencyError(RuntimeError):
    """Raised when task dependencies cannot be resolved."""

class TaskIncomplete(RuntimeError):
    """Raised by :meth:`Task.result` when the task did not complete."""

class TaskError(RuntimeError):
    """Raised by :meth:`Task.result` when the task errored without raised an exception."""


class taskproperty(object):

    def __init__(self, name, default=None):
        self.name = name
        self.default = default

    def __get__(self, obj, cls):
        if obj is None:
            return self
        try:
            return obj._store[self.name]
        except KeyError:
            if self.default:
                return obj._store.setdefault(self.name, self.default())
            raise AttributeError(self.name)

    def __set__(self, obj, value):
        if obj.is_frozen:
            raise RuntimeError('task has been frozen')
        else:
            obj._store[self.name] = value


class Task(object):

    pattern = taskproperty('pattern')
    func = taskproperty('func')
    args = taskproperty('args')
    kwargs = taskproperty('kwargs')

    children = taskproperty('children', default=list)
    dependencies = taskproperty('dependencies', default=list)

    status = taskproperty('status')



    def __init__(self, func=None, args=None, kwargs=None, **extra):

        self.id = None
        self.is_frozen = False

        self._store = {}

        self.pattern = 'generic'        
        self.func = func
        self.args = list(args or ())
        self.kwargs = dict(kwargs or {})

        self.status = 'pending'

        for k, v in extra.iteritems():
            if hasattr(self, k):
                setattr(self, k, v)
            else:
                self[k] = v

    def result(self, strict=True):
        """Retrieve the results of running this task.

        :returns: The result of running the task.
        :raises: :class:`TaskIncomplete` if incomplete, 
            :class:`TaskError` if the task errored,
            or any exception that the execution of the task may have raised.

        """

        if self.status == 'success':
            return self._store.get('result')

        elif not strict:
            return

        elif self.status == 'error':
            exc = self._store.get('exception')
            if exc:
                raise exc
            message = '{} from {}'.format(self._store.get('error', 'unknown'), self.id)
            type_ = self._store.get('error_type', TaskError)
            if isinstance(type_, basestring):
                type_ = getattr(__builtins__, type_, TaskError)
            raise type_(message)

        else:
            raise TaskIncomplete('task is %s' % self.status)

    def error(self, message):
        """Signal that the task has errored while running.

        :returns: A :class:`TaskError` that can be raised.

        """

        self._store['status'] = 'error'
        self._store['error'] = message

        # TODO: publish on redis
        return TaskError(message)

    def complete(self, result=None):
        """Signal that the task has completed running."""

        self._store['status'] = 'success'
        self._store['result'] = result
        # TODO: publish on redis

    def assert_graph_ids(self, base=None, index=1, _visited=None):
        """Make sure the entire DAG rooted at this point has IDs."""

        _visited = _visited or set()
        if self in _visited:
            return
        _visited.add(self)

        if self.id is None:
            self.id = (base + '.' if base else '') + str(index)

        for i, dep in enumerate(self.dependencies):
            dep.assert_graph_ids(base, index+1, _visited)
        for i, child in enumerate(self.children):
            child.assert_graph_ids(self.id, 1, _visited)

    def _iter_linearized(self, _visited=None):

        _visited = _visited or set()
        if (self, 'incomplete') in _visited:
            raise DependencyError('cycle in dependencies with %r' % self.id)
        if self in _visited:
            return

        _visited.add((self, 'incomplete'))
        _visited.add(self)

        for x in self.dependencies:
            for y in x._iter_linearized(_visited):
                yield y
        for x in self.children:
            for y in x._iter_linearized(_visited):
                yield y

        _visited.remove((self, 'incomplete'))
        yield self

    def run(self):
        self.assert_graph_ids()
        for task in list(self._iter_linearized()):
            task._run()
        return self._store.setdefault('result', None)

    def _run(self):
        """Find the pattern handler, call it, and catch errors."""

        pattern_name = self.pattern
        pattern_func = aque.patterns.registry.get(pattern_name, pattern_name)
        pattern_func = decode_callable(pattern_func)

        if pattern_func is None:
            raise ValueError('no aque pattern %r' % pattern_name)

        # log.debug('handling task %r with %r' % (self['id'], pattern))
        
        try:
            pattern_func(self)
        except Exception as e:
            self.status = 'error'
            if e.args:
                self._store['error'] = e.args[0]
            self._store['error_type'] = '{}.{}'.format(
                e.__class__.__module__,
                e.__class__.__name__,
            )
            self._store['exception'] = e
            raise


    def progress(self, value, max=None, message=None):
        """Notify the web UI of progress."""

        if value is not None:
            self['progress'] = value
        if max is not None:
            self['progress_max'] = max
        if message is not None:
            self['progress_message'] = message

        # TODO: publish to some channel

    def ping(self):
        """Notify the other workers that this task is not dead."""
        # TODO: do something.
    



